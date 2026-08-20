"""
Standalone training subprocess for LSTM Autoencoder.
Runs outside Gunicorn workers so training is not killed when workers recycle.
Invoked by main.py when learning is disabled: subprocess.Popen([python, '-m', 'app.train_worker', log_type]).
Writes training_done_<log_type>.json or training_failed_<log_type>.json on exit for API to pick up.
"""
import os
import sys
import json
import logging
import numpy as np
from datetime import datetime

# Same config as main.py (unified 33-feature model)
LOG_TYPES = {
    'http': {'input_dim': 33, 'timesteps': 10, 'encoding_dim': 16},
    'ssl':  {'input_dim': 33, 'timesteps': 10, 'encoding_dim': 16},
    'dns':  {'input_dim': 33, 'timesteps': 10, 'encoding_dim': 16},
    'conn': {'input_dim': 33, 'timesteps': 10, 'encoding_dim': 16},
}

# Result files so API can detect completion (writable dir, not model dir)
RESULT_DIR = '/app/data'


def progress_path(log_type):
    return os.path.join(RESULT_DIR, f'training_progress_{log_type}.json')


class FileProgressCallback:
    """
    Writes per-epoch progress where the API can read it.

    Training runs in this subprocess, so the API's in-memory training_progress dict is
    never updated by it - /training/progress used to report 'initializing' for the whole
    run. Keras Callback is imported lazily so importing this module stays cheap.
    """

    def __new__(cls, log_type, total_epochs):
        from tensorflow.keras.callbacks import Callback

        class _Cb(Callback):
            def __init__(self):
                super().__init__()
                self.log_type = log_type
                self.total_epochs = total_epochs
                self.started = None

            def _write(self, payload):
                try:
                    tmp = progress_path(self.log_type) + '.tmp'
                    with open(tmp, 'w') as f:
                        json.dump(payload, f)
                    os.replace(tmp, progress_path(self.log_type))
                except OSError as e:
                    logging.getLogger('train_worker').warning("progress write failed: %s", e)

            def on_train_begin(self, logs=None):
                self.started = datetime.now()
                self._write({'status': 'training', 'current_epoch': 0,
                             'total_epochs': self.total_epochs, 'progress_pct': 0,
                             'started_at': self.started.isoformat()})

            def on_epoch_end(self, epoch, logs=None):
                logs = logs or {}
                elapsed = (datetime.now() - self.started).total_seconds()
                done = epoch + 1
                eta = (elapsed / done) * (self.total_epochs - done)
                self._write({
                    'status': 'training',
                    'current_epoch': done,
                    'total_epochs': self.total_epochs,
                    'loss': float(logs.get('loss', 0.0)),
                    'val_loss': float(logs.get('val_loss', 0.0)),
                    'progress_pct': int(done / max(self.total_epochs, 1) * 100),
                    'elapsed_seconds': int(elapsed),
                    'eta_seconds': int(eta),
                    'started_at': self.started.isoformat(),
                })

            def on_train_end(self, logs=None):
                self._write({'status': 'completed', 'total_epochs': self.total_epochs,
                             'progress_pct': 100})

        return _Cb()


def main():
    if len(sys.argv) < 2:
        print("Usage: python -m app.train_worker <log_type>", file=sys.stderr)
        sys.exit(2)
    log_type = sys.argv[1].lower()
    if log_type not in LOG_TYPES:
        print(f"Invalid log_type: {log_type}", file=sys.stderr)
        sys.exit(2)

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        stream=sys.stderr,
    )
    logger = logging.getLogger('train_worker')

    result_file_done = os.path.join(RESULT_DIR, f'training_done_{log_type}.json')
    result_file_fail = os.path.join(RESULT_DIR, f'training_failed_{log_type}.json')
    for f in (result_file_done, result_file_fail):
        if os.path.exists(f):
            try:
                os.unlink(f)
            except OSError:
                pass

    try:
        from app.database import Database
        from app.model import LSTMAutoencoder
    except Exception as e:
        logger.exception("Failed to import app modules")
        _write_result(result_file_fail, {'status': 'error', 'error': str(e)})
        sys.exit(1)

    db = Database()
    config = LOG_TYPES[log_type]

    data = db.load_collected_data(log_type)
    if data is None:
        msg = f'No collected data for {log_type}'
        logger.warning(msg)
        _write_result(result_file_fail, {'status': 'error', 'error': msg})
        sys.exit(1)

    if len(data.shape) == 1:
        data = data.reshape(1, -1)
    current_rows = data.shape[0]
    if current_rows < config['timesteps']:
        msg = f'Insufficient data for {log_type}: {current_rows} rows, need at least {config["timesteps"]}'
        logger.warning(msg)
        _write_result(result_file_fail, {'status': 'error', 'error': msg})
        sys.exit(1)

    try:
        total_rows = db.update_total_rows(current_rows, log_type)
    except Exception as e:
        logger.warning("update_total_rows failed: %s", e)
        total_rows = current_rows

    n_samples = len(data) - config['timesteps'] + 1
    logger.info("Creating %d training sequences for %s...", n_samples, log_type)
    X = np.zeros((n_samples, config['timesteps'], config['input_dim']), dtype=np.float32)
    for i in range(n_samples):
        X[i] = data[i:i + config['timesteps']]

    logger.info("Training data shape: %s, Memory: %.2f MB", X.shape, X.nbytes / (1024**2))

    model = LSTMAutoencoder(
        log_type=log_type,
        input_dim=config['input_dim'],
        timesteps=config['timesteps'],
        encoding_dim=config['encoding_dim'],
    )
    EPOCHS = 100
    training_result = model.train(
        data=X,
        epochs=EPOCHS,
        batch_size=64,
        validation_split=0.2,
        progress_callback=FileProgressCallback(log_type, EPOCHS),
    )

    if training_result.get('status') != 'success':
        msg = training_result.get('message', str(training_result))
        logger.error("Training failed for %s: %s", log_type, msg)
        _write_result(result_file_fail, {'status': 'error', 'error': msg})
        sys.exit(1)

    metrics = training_result.get('metrics', {})
    db.save_model_info(
        log_type=log_type,
        model_path=training_result['model_path'],
        input_dim=config['input_dim'],
        timesteps=config['timesteps'],
        training_samples=n_samples,
        validation_loss=training_result['validation_loss'],
        training_loss=training_result['training_loss'],
        anomaly_threshold=training_result.get('anomaly_threshold'),
        accuracy=metrics.get('accuracy'),
        precision=metrics.get('precision'),
        recall=metrics.get('recall'),
        f1_score=metrics.get('f1_score'),
        fpr=metrics.get('false_positive_rate'),
    )
    db.clear_collected_data(log_type)
    logger.info("Model trained and saved for %s", log_type)

    _write_result(result_file_done, {
        'status': 'success',
        'log_type': log_type,
        'model_path': training_result['model_path'],
        'data_size': current_rows,
        'total_rows': total_rows,
        'training_samples': n_samples,
        'validation_loss': training_result['validation_loss'],
        'training_loss': training_result['training_loss'],
        'epochs_trained': training_result.get('epochs_trained'),
        'anomaly_threshold': training_result.get('anomaly_threshold'),
        'completed_at': datetime.utcnow().isoformat() + 'Z',
    })
    sys.exit(0)


def _write_result(path, obj):
    try:
        with open(path, 'w') as f:
            json.dump(obj, f, indent=2)
    except Exception as e:
        logging.getLogger('train_worker').warning("Could not write result file %s: %s", path, e)


if __name__ == '__main__':
    main()
