"""
LSTM Autoencoder model for anomaly detection.
"""
import os
import json
import pickle
import time
import tempfile
import fcntl
import numpy as np
import tensorflow as tf
from sklearn.preprocessing import MinMaxScaler
from tensorflow.keras.models import Sequential, load_model
from tensorflow.keras.layers import LSTM, Dense, Dropout
from tensorflow.keras.callbacks import EarlyStopping, ModelCheckpoint, ReduceLROnPlateau
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

# Retry configuration for model/scaler load (handles concurrent access)
LOAD_RETRY_ATTEMPTS = 5
LOAD_RETRY_DELAY_BASE = 0.5  # seconds
LOAD_RETRY_DELAY_MAX = 5.0   # seconds

# ─── Thread management ────────────────────────────────────────────────────────
# Two modes:
#   INFERENCE: moderate threads per worker (env TF_NUM_INTRAOP_THREADS / nproc/workers)
#   TRAINING:  all available cores for the single worker doing training
#
# TF thread counts CAN be changed at runtime (tf.config.threading); change takes
# effect at the next TF op (eager mode). We switch to TRAINING mode at the start
# of train() and restore INFERENCE mode when it finishes/fails.

import multiprocessing as _mp

def _inference_threads():
    """Moderate intra_op threads for inference: nproc/workers or env."""
    intra = int(os.environ.get('TF_NUM_INTRAOP_THREADS', 0))
    inter = int(os.environ.get('TF_NUM_INTEROP_THREADS', 0))
    if intra <= 0:
        workers = int(os.environ.get('GUNICORN_WORKERS', 4))
        cores   = _mp.cpu_count()
        intra   = max(2, min(8, cores // max(workers, 1)))
    if inter <= 0:
        inter = 4
    return inter, intra

def _training_threads():
    """All available cores for training (single worker)."""
    cores  = _mp.cpu_count()
    intra  = min(cores, 64)   # all cores, cap 64
    inter  = 4                # low inter_op; training is intra_op heavy
    return inter, intra

def _set_tf_threads(inter, intra, label):
    try:
        tf.config.threading.set_inter_op_parallelism_threads(inter)
        tf.config.threading.set_intra_op_parallelism_threads(intra)
        logger.info(f"[threads:{label}] inter_op={inter}  intra_op={intra}")
    except Exception as e:
        logger.warning(f"Could not set TF threads ({label}): {e}")

# ─── Configure TensorFlow to use CPU only (no GPU) ────────────────────────────
def configure_tensorflow():
    """Configure TensorFlow for CPU-only, inference thread count at startup."""
    try:
        tf.config.set_visible_devices([], 'GPU')
        logger.info("TensorFlow configured for CPU only (GPU disabled)")
    except Exception as e:
        logger.warning(f"Could not set CPU-only mode: {e}")

    inter, intra = _inference_threads()
    _set_tf_threads(inter, intra, "inference-startup")

# Configure on module import
configure_tensorflow()


def _lock_file(fd, exclusive=True):
    """Acquire lock on file descriptor. exclusive=True for write, False for read."""
    try:
        fcntl.flock(fd, fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH)
        return True
    except (IOError, OSError) as e:
        logger.warning(f"Failed to acquire file lock: {e}")
        return False


def _unlock_file(fd):
    """Release lock on file descriptor."""
    try:
        fcntl.flock(fd, fcntl.LOCK_UN)
    except (IOError, OSError):
        pass


def _with_dir_lock(model_dir, exclusive, fn, *args, **kwargs):
    """
    Run fn(*args, **kwargs) while holding a lock on model_dir/.lock.
    exclusive=True for writes (training/save), False for reads (load).
    """
    lock_path = os.path.join(model_dir, '.lock')
    os.makedirs(model_dir, exist_ok=True)
    with open(lock_path, 'a') as lock_f:
        if not _lock_file(lock_f.fileno(), exclusive=exclusive):
            raise IOError("Could not acquire directory lock")
        try:
            return fn(*args, **kwargs)
        finally:
            _unlock_file(lock_f.fileno())


def _atomic_write_pickle(path, obj):
    """Write pickle to temp file, then atomically rename to final path."""
    fd, tmp_path = tempfile.mkstemp(
        dir=os.path.dirname(path),
        prefix=os.path.basename(path) + '.',
        suffix='.tmp'
    )
    try:
        os.close(fd)
        with open(tmp_path, 'wb') as f:
            pickle.dump(obj, f)
        os.rename(tmp_path, path)
    except Exception:
        if os.path.exists(tmp_path):
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
        raise


class LSTMAutoencoder:

    def __init__(self, log_type, input_dim, timesteps, encoding_dim=64):

        self.log_type = log_type
        self.input_dim = input_dim
        self.timesteps = timesteps
        self.encoding_dim = encoding_dim

        self.model = None
        self.model_path = None

        self.scaler = MinMaxScaler()
        self.scaler_path = None

        self.threshold = None
        self.threshold_method = None

        self.model_dir = os.path.join(
            os.environ.get('MODEL_PATH', '/app/models'),
            log_type
        )

        os.makedirs(self.model_dir, exist_ok=True)

        self.load_latest_model()

    # =========================================================
    # Model Architecture (BiLSTM + Residual + LayerNorm)
    # =========================================================

    def _build_model(self):

        from tensorflow.keras.layers import (
            Input, LSTM, Dense, Dropout,
            RepeatVector, TimeDistributed,
            Bidirectional, LayerNormalization,
            Add
        )

        inputs = Input(shape=(self.timesteps, self.input_dim))

        x = Bidirectional(
            LSTM(64, return_sequences=True)
        )(inputs)

        x = LayerNormalization()(x)

        res = x

        x = LSTM(32, return_sequences=False)(x)

        encoded = Dense(self.encoding_dim)(x)

        x = RepeatVector(self.timesteps)(encoded)

        x = LSTM(32, return_sequences=True)(x)

        x = Bidirectional(
            LSTM(64, return_sequences=True)
        )(x)

        x = LayerNormalization()(x)

        x = Add()([x, res])

        outputs = TimeDistributed(
            Dense(self.input_dim)
        )(x)

        model = tf.keras.Model(inputs, outputs)

        model.compile(
            optimizer='adam',
            loss='mse',
            metrics=['mae']
        )

        return model

    # =========================================================
    # Scaling helpers
    # =========================================================

    def _scale_fit_transform(self, data):

        flat = data.reshape(-1, self.input_dim)

        flat_scaled = self.scaler.fit_transform(flat)

        return flat_scaled.reshape(
            data.shape[0],
            self.timesteps,
            self.input_dim
        )

    def _scale_transform(self, data):

        flat = data.reshape(-1, self.input_dim)

        flat_scaled = self.scaler.transform(flat)

        return flat_scaled.reshape(
            data.shape[0],
            self.timesteps,
            self.input_dim
        )

    # =========================================================
    # EVT threshold
    # =========================================================

    def _compute_threshold(self, errors):

        try:

            from scipy.stats import genpareto

            q = np.percentile(errors, 95)

            tail = errors[errors >= q] - q

            if len(tail) < 30:
                raise Exception("tail too small")

            c, loc, scale = genpareto.fit(tail)

            threshold = q + genpareto.ppf(
                0.999,
                c,
                loc=loc,
                scale=scale
            )

            self.threshold_method = "EVT"

            logger.info(f"Threshold EVT: {threshold}")

            return float(threshold)

        except Exception as e:

            q75, q25 = np.percentile(errors, [75, 25])

            iqr = q75 - q25

            threshold = q75 + 1.5 * iqr

            self.threshold_method = "IQR"

            logger.warning(
                f"EVT failed -> fallback IQR threshold: {threshold}"
            )

            return float(threshold)

    # =========================================================
    # Train
    # =========================================================

    def train(self, data, epochs=100, batch_size=32,
              validation_split=0.2, progress_callback=None):

        try:

            logger.info("Scaling training data")

            data_scaled = self._scale_fit_transform(data)

            self.model = self._build_model()

            history = self.model.fit(
                data_scaled,
                data_scaled,
                epochs=epochs,
                batch_size=batch_size,
                validation_split=validation_split,
                verbose=1,
                shuffle=False
            )

            preds = self.model.predict(data_scaled)

            mae = np.mean(
                np.abs(data_scaled - preds),
                axis=(1, 2)
            )

            self.threshold = self._compute_threshold(mae)

            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

            model_path = os.path.join(
                self.model_dir,
                f'lstm_autoencoder_{self.log_type}_{timestamp}.keras'
            )

            scaler_path = os.path.join(
                self.model_dir,
                f'scaler_{self.log_type}_{timestamp}.pkl'
            )

            threshold_path = os.path.join(
                self.model_dir,
                f'threshold_{self.log_type}_{timestamp}.json'
            )

            self.model.save(model_path)

            with open(scaler_path, 'wb') as f:
                pickle.dump(self.scaler, f)

            with open(threshold_path, 'w') as f:
                json.dump({
                    "threshold": self.threshold,
                    "method": self.threshold_method
                }, f)

            self.model_path = model_path

            return {
                'status': 'success',
                'model_path': model_path,
                'timestamp': timestamp,
                'validation_loss': float(
                    np.min(history.history['val_loss'])
                ),
                'training_loss': float(
                    np.min(history.history['loss'])
                ),
                'epochs_trained': len(history.history['loss']),
                'anomaly_threshold': self.threshold
            }

        except Exception as e:

            logger.error(str(e))

            return {
                'status': 'error',
                'message': str(e)
            }

    # =========================================================
    # Predict (FIXED scaling)
    # =========================================================

    def predict(self, data):

        if self.model is None:
            raise ValueError("Model not loaded")

        data_scaled = self._scale_transform(data)

        predictions = self.model.predict(data_scaled)

        mse = np.mean(
            np.square(data_scaled - predictions),
            axis=(1, 2)
        )

        mae = np.mean(
            np.abs(data_scaled - predictions),
            axis=(1, 2)
        )

        return predictions, mse, mae

    # =========================================================
    # Load model
    # =========================================================

    def load_latest_model(self):

        files = [
            f for f in os.listdir(self.model_dir)
            if f.endswith(".keras")
        ]

        if not files:
            return None

        files.sort(reverse=True)

        model_file = files[0]

        timestamp = model_file.split("_")[-1].replace(".keras", "")

        scaler_file = f"scaler_{self.log_type}_{timestamp}.pkl"

        threshold_file = f"threshold_{self.log_type}_{timestamp}.json"

        self.model_path = os.path.join(self.model_dir, model_file)

        self.model = load_model(self.model_path)

        scaler_path = os.path.join(self.model_dir, scaler_file)

        if os.path.exists(scaler_path):
            with open(scaler_path, "rb") as f:
                self.scaler = pickle.load(f)

        threshold_path = os.path.join(
            self.model_dir,
            threshold_file
        )

        if os.path.exists(threshold_path):
            with open(threshold_path) as f:
                th = json.load(f)
                self.threshold = th["threshold"]
                self.threshold_method = th.get("method")

        return self.model_path

    # =========================================================
    # Model info
    # =========================================================

    def get_model_info(self):

        if self.model is None:
            return None

        layers = []

        for layer in self.model.layers:

            try:
                shape = layer.output_shape
            except:
                shape = None

            layers.append({
                "name": layer.name,
                "type": layer.__class__.__name__,
                "output_shape": shape
            })

        return {

            "log_type": self.log_type,
            "input_dim": self.input_dim,
            "timesteps": self.timesteps,
            "encoding_dim": self.encoding_dim,
            "model_path": self.model_path,
            "threshold": self.threshold,
            "threshold_method": self.threshold_method,
            "layers": layers
        }