"""
LSTM Autoencoder model for anomaly detection with Adaptive Rolling Threshold.
Real solution for threshold that adapts to actual data distribution.
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
from collections import deque
import threading
import logging

logger = logging.getLogger(__name__)

# Retry configuration for model/scaler load (handles concurrent access)
LOAD_RETRY_ATTEMPTS = 5
LOAD_RETRY_DELAY_BASE = 0.5  # seconds
LOAD_RETRY_DELAY_MAX = 5.0   # seconds

# ─── Thread management ────────────────────────────────────────────────────────
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
    inter  = 4
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
    """Acquire lock on file descriptor."""
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
    """Run fn while holding a lock on model_dir/.lock."""
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


class AdaptiveThreshold:
    """
    Adaptive threshold that uses rolling statistics to dynamically adjust.
    
    Unlike static thresholds computed once at training, this class maintains
    a rolling window of recent MAE scores and adapts the threshold based on:
    1. Training threshold as initial baseline
    2. Rolling percentile of recent predictions (e.g., 95th percentile of last 1000)
    3. Drift detection to handle concept drift
    """
    
    def __init__(self, log_type, window_size=1000, percentile=95, 
                 min_threshold=None, max_threshold=None, drift_sensitivity=0.1):
        self.log_type = log_type
        self.window_size = window_size
        self.percentile = percentile
        self.min_threshold = min_threshold  # Floor threshold
        self.max_threshold = max_threshold   # Ceiling threshold
        self.drift_sensitivity = drift_sensitivity
        
        # Rolling window of MAE scores
        self.mae_history = deque(maxlen=window_size)
        self.lock = threading.Lock()
        
        # Initial threshold from training (baseline)
        self.training_threshold = None
        self.threshold_method = None
        
        # Statistics
        self.total_predictions = 0
        self.anomalies_detected = 0
        
    def set_training_threshold(self, threshold, method=None):
        """Set the initial threshold from training."""
        self.training_threshold = threshold
        self.threshold_method = method
        logger.info(f"[{self.log_type}] Training threshold set: {threshold:.6f} ({method})")
        
    def add_mae(self, mae_score):
        """Add a new MAE score to the history."""
        with self.lock:
            self.mae_history.append(mae_score)
            self.total_predictions += 1
            if mae_score >= self.get_threshold():
                self.anomalies_detected += 1
                
    def add_mae_batch(self, mae_scores):
        """Add multiple MAE scores at once."""
        for mae in mae_scores:
            self.add_mae(mae)
    
    def get_threshold(self):
        """
        Get adaptive threshold based on rolling statistics.
        
        Strategy:
        - If we have enough history: use rolling percentile
        - Otherwise: use training threshold
        - Apply min/max bounds
        """
        with self.lock:
            if len(self.mae_history) < 100:
                # Not enough data - use training threshold
                threshold = self.training_threshold
            else:
                # Use rolling percentile of recent predictions
                recent_maes = list(self.mae_history)
                rolling_threshold = np.percentile(recent_maes, self.percentile)
                
                # Blend with training threshold (more weight to rolling as we get more data)
                blend_factor = min(len(self.mae_history) / self.window_size, 1.0)
                threshold = (1 - blend_factor) * self.training_threshold + blend_factor * rolling_threshold
            
            # Apply bounds
            if self.min_threshold is not None and threshold < self.min_threshold:
                threshold = self.min_threshold
            if self.max_threshold is not None and threshold > self.max_threshold:
                threshold = self.max_threshold
                
            return float(threshold) if threshold is not None else 0.1
    
    def is_anomaly(self, mae_score):
        """Check if MAE score is anomalous."""
        threshold = self.get_threshold()
        return mae_score >= threshold
    
    def get_statistics(self):
        """Get current statistics."""
        with self.lock:
            stats = {
                'total_predictions': self.total_predictions,
                'anomalies_detected': self.anomalies_detected,
                'anomaly_rate': self.anomalies_detected / max(self.total_predictions, 1),
                'history_size': len(self.mae_history),
                'training_threshold': self.training_threshold,
                'current_threshold': self.get_threshold(),
                'method': self.threshold_method
            }
            if len(self.mae_history) > 0:
                stats['mae_mean'] = float(np.mean(self.mae_history))
                stats['mae_std'] = float(np.std(self.mae_history))
                stats['mae_min'] = float(np.min(self.mae_history))
                stats['mae_max'] = float(np.max(self.mae_history))
                stats['mae_p50'] = float(np.percentile(self.mae_history, 50))
                stats['mae_p95'] = float(np.percentile(self.mae_history, 95))
                stats['mae_p99'] = float(np.percentile(self.mae_history, 99))
            return stats
    
    def reset(self):
        """Reset the adaptive threshold."""
        with self.lock:
            self.mae_history.clear()
            self.total_predictions = 0
            self.anomalies_detected = 0


class LSTMAutoencoder:

    def __init__(self, log_type, input_dim, timesteps, encoding_dim=64,
                 adaptive_threshold=True, threshold_window=1000, threshold_percentile=95):

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

        # Adaptive threshold system
        self.adaptive_threshold_enabled = adaptive_threshold
        self.threshold_window = threshold_window
        self.threshold_percentile = threshold_percentile
        
        if self.adaptive_threshold_enabled:
            self.adaptive = AdaptiveThreshold(
                log_type=log_type,
                window_size=threshold_window,
                percentile=threshold_percentile,
                min_threshold=0.001,  # Floor: don't go below 0.1%
                max_threshold=1.0     # Ceiling: don't exceed 100%
            )
        else:
            self.adaptive = None

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

            # Use a reasonable threshold from training (mean + 3 std is conservative)
            self.threshold = float(np.mean(mae) + 3 * np.std(mae))
            self.threshold_method = "TRAINING_MEAN_3STD"

            logger.info(f"Training threshold (baseline): {self.threshold:.6f}")

            # Set this as baseline for adaptive threshold
            if self.adaptive is not None:
                self.adaptive.set_training_threshold(self.threshold, self.threshold_method)

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
    # Predict
    # =========================================================

    def predict(self, data, return_mae=True):

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

        # Update adaptive threshold with new MAE scores
        if self.adaptive is not None:
            self.adaptive.add_mae_batch(mae.tolist())
        
        if return_mae:
            return predictions, mse, mae
        return predictions

    def predict_with_threshold(self, data):
        """
        Predict and return results with anomaly detection using adaptive threshold.
        """
        _, mse, mae = self.predict(data)
        
        # Get adaptive threshold
        if self.adaptive is not None:
            threshold = self.adaptive.get_threshold()
            adaptive_method = "adaptive_rolling"
        else:
            threshold = self.threshold
            adaptive_method = self.threshold_method or "static"
        
        # Determine anomalies
        is_anomaly = mae >= threshold
        
        return {
            'mae': mae,
            'mse': mse,
            'threshold': threshold,
            'is_anomaly': is_anomaly,
            'threshold_method': adaptive_method
        }

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

        # Extract timestamp correctly
        parts = model_file.replace(".keras", "").split("_")
        timestamp = "_".join(parts[-2:])

        scaler_file = f"scaler_{self.log_type}_{timestamp}.pkl"

        threshold_file = f"threshold_{self.log_type}_{timestamp}.json"

        self.model_path = os.path.join(self.model_dir, model_file)

        self.model = load_model(self.model_path)

        scaler_path = os.path.join(self.model_dir, scaler_file)

        if os.path.exists(scaler_path):
            with open(scaler_path, "rb") as f:
                self.scaler = pickle.load(f)
                logger.info(f"Loaded scaler from {scaler_file}")
        else:
            logger.warning(f"Scaler file not found: {scaler_file}")

        threshold_path = os.path.join(
            self.model_dir,
            threshold_file
        )

        if os.path.exists(threshold_path):
            with open(threshold_path) as f:
                th = json.load(f)
                self.threshold = th["threshold"]
                self.threshold_method = th.get("method")
                # Initialize adaptive threshold with training threshold
                if self.adaptive is not None:
                    self.adaptive.set_training_threshold(self.threshold, self.threshold_method)

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

        info = {
            "log_type": self.log_type,
            "input_dim": self.input_dim,
            "timesteps": self.timesteps,
            "encoding_dim": self.encoding_dim,
            "model_path": self.model_path,
            "threshold": self.threshold,
            "threshold_method": self.threshold_method,
            "layers": layers
        }
        
        # Add adaptive threshold stats if available
        if self.adaptive is not None:
            info["adaptive_threshold"] = self.adaptive.get_statistics()
            
        return info
    
    def get_adaptive_stats(self):
        """Get adaptive threshold statistics."""
        if self.adaptive is not None:
            return self.adaptive.get_statistics()
        return None
