"""
Main Flask application for the LSTM Autoencoder API.
"""
from flask import Flask, request, jsonify
import os
import sys
import json
import numpy as np
from datetime import datetime
import logging
from logging.handlers import RotatingFileHandler
import threading
import subprocess
from .model import LSTMAutoencoder
from .database import Database
from collections import deque
from tensorflow.keras.callbacks import Callback

# Configure logging
def setup_logging():
    log_dir = '/app/data'
    log_file = os.path.join(log_dir, 'lstm_api.log')
    os.makedirs(log_dir, exist_ok=True)
    
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(processName)s - %(process)d - %(levelname)s - %(message)s'
    )
    
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=10*1024*1024,
        backupCount=5
    )
    file_handler.setFormatter(formatter)
    
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(stream_handler)
    
    return logging.getLogger(__name__)

logger = setup_logging()

# Define log type configurations
# UNIFIED FEATURE ENGINEERING - All log types use 33 features (NetworkAnomalyPreprocessor)
# NEW MODEL: Simpler architecture from train_model_01.py (latent_dim=16)
LOG_TYPES = {
    'http': {
        'input_dim': 33,  # Unified feature count (all log types)
        'timesteps': 10,  # Matches preprocessor window_size
        'encoding_dim': 16  # Latent dimension (simpler model)
    },
    'ssl': {
        'input_dim': 33,  # Unified feature count (all log types)
        'timesteps': 10,  # Matches preprocessor window_size
        'encoding_dim': 16  # Latent dimension (simpler model)
    },
    'dns': {
        'input_dim': 33,  # Unified feature count (all log types)
        'timesteps': 10,  # Matches preprocessor window_size
        'encoding_dim': 16  # Latent dimension (simpler model)
    },
    'conn': {
        'input_dim': 33,  # Unified feature count (all log types)
        'timesteps': 10,  # Matches preprocessor window_size
        'encoding_dim': 16  # Latent dimension (simpler model)
    }
}

# In-memory buffer metrics reported by Flink (per log type)
BUFFER_STATUS = {lt: {'buffer_size': 0, 'updated_at': None} for lt in LOG_TYPES.keys()}

def create_app():
    app = Flask(__name__)
    
    # Initialize database
    db = Database()
    
    # Initialize models for each log type
    models = {}
    for log_type, config in LOG_TYPES.items():
        models[log_type] = LSTMAutoencoder(
            log_type=log_type,
            input_dim=config['input_dim'],
            timesteps=config['timesteps'],
            encoding_dim=config['encoding_dim']
        )
    
    # Sliding window buffers for each log type (for proper time-series prediction)
    time_windows = {log_type: deque(maxlen=config['timesteps']) 
                    for log_type, config in LOG_TYPES.items()}
    
    logger.info("LSTM API application starting up")
    
    @app.route('/health', methods=['GET'])
    def health_check():
        """Health check endpoint for service availability verification"""
        try:
            # Check if database is accessible
            db_status = db.check_connection()
            # Check if models are initialized
            models_status = all(model is not None for model in models.values())
            
            return jsonify({
                "status": "ok" if db_status and models_status else "degraded",
                "service": "lstm-autoencoder",
                "version": "1.0.0",
                "components": {
                    "database": "ok" if db_status else "error",
                    "models": "ok" if models_status else "error"
                }
            }), 200 if db_status and models_status else 503
        except Exception as e:
            logger.error(f"Health check failed: {str(e)}")
            return jsonify({
                "status": "error",
                "service": "lstm-autoencoder",
                "version": "1.0.0",
                "error": str(e)
            }), 503
    
    def get_learning_status(log_type):
        return db.get_learning_status(log_type)
    
    def set_learning_status(enabled, log_type):
        try:
            success = db.set_learning_status(enabled, log_type)
            if not success:
                raise Exception(f"Failed to update learning status for {log_type} in database")
            logger.info(f"Learning mode {'enabled' if enabled else 'disabled'} for {log_type}")
        except Exception as e:
            logger.error(f"Error setting learning status for {log_type}: {e}")
            raise
    
    def save_collected_data(data, log_type):
        """Save collected data to database"""
        try:
            success = db.save_collected_data(data, log_type)
            if not success:
                raise Exception(f"Failed to save {log_type} data to database")
            logger.info(f"Saved collected {log_type} data with shape: {data.shape}")
        except Exception as e:
            logger.error(f"Error saving collected {log_type} data: {e}")
            raise
    
    def load_collected_data(log_type):
        """Load collected data from database"""
        try:
            data = db.load_collected_data(log_type)
            if data is not None:
                logger.info(f"Loaded collected {log_type} data with shape: {data.shape}")
            return data
        except Exception as e:
            logger.error(f"Error loading collected {log_type} data: {e}")
            return None
    
    def get_total_rows(log_type):
        """Get the total number of rows processed for a log type"""
        return db.get_total_rows(log_type)

    def update_total_rows(new_rows, log_type):
        """Update the total number of rows processed for a log type"""
        return db.update_total_rows(new_rows, log_type)
    
    # Track training status and progress per log type
    training_status = {}
    training_progress = {}  # Real-time training progress
    training_lock = threading.Lock()
    TRAINING_RESULT_DIR = '/app/data'
    # Sequential training: one log type at a time (FIFO queue). Uses full CPU for the single active job.
    training_pending_queue = deque()

    def _is_any_training_running():
        """True if any log type has an active training subprocess. Call with training_lock held."""
        for lt, st in training_status.items():
            if st.get('status') != 'training':
                continue
            pid = st.get('pid')
            if pid is None:
                return True  # starting up
            try:
                os.kill(pid, 0)
                return True  # process alive
            except (ProcessLookupError, PermissionError):
                pass
        return False

    def _start_training_subprocess(log_type):
        """
        Start training subprocess for one log type. Only call when no other training is running
        (or caller is the queue drain). Returns 'started', 'queued', or 'insufficient_data'.
        """
        data_count = db.get_collected_data_count(log_type)
        logger.info(f"_start_training_subprocess called for {log_type}, data_count={data_count}, required={LOG_TYPES[log_type]['timesteps']}")
        if data_count < LOG_TYPES[log_type]['timesteps']:
            logger.warning(f"Insufficient data for training {log_type}: have {data_count}, need {LOG_TYPES[log_type]['timesteps']}")
            return 'insufficient_data'
        with training_lock:
            if _is_any_training_running():
                if log_type not in training_pending_queue:
                    training_pending_queue.append(log_type)
                    logger.info(f"Training queued for {log_type} ({len(training_pending_queue)} in queue)")
                return 'queued'
            training_status[log_type] = {
                'status': 'training',
                'started_at': datetime.now().isoformat(),
                'pid': None,
            }
            training_progress[log_type] = {'status': 'initializing', 'progress_pct': 0}
        cwd = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        training_log = os.path.join(TRAINING_RESULT_DIR, f'training_{log_type}.log')
        try:
            log_f = open(training_log, 'w')
        except OSError as e:
            log_f = None
            logger.warning("Cannot open training log %s: %s", training_log, e)
        # Use LSTM_TRAINING_CPUS (set by configure-resources.sh) so training uses all available CPUs; no Python-side cap
        import multiprocessing as _mp
        max_cpus = int(os.environ.get('LSTM_TRAINING_CPUS', 0)) or getattr(os, 'cpu_count', lambda: None)() or _mp.cpu_count() or 256
        max_cpus = max(1, max_cpus)  # no upper cap so host/container limit is the only limit
        train_env = os.environ.copy()
        train_env['OMP_NUM_THREADS'] = str(max_cpus)
        train_env['MKL_NUM_THREADS'] = str(max_cpus)
        train_env['TF_NUM_INTRAOP_THREADS'] = str(max_cpus)
        train_env['LSTM_TRAINING_CPUS'] = str(max_cpus)
        train_env['TF_NUM_INTEROP_THREADS'] = '4'
        proc = subprocess.Popen(
            [sys.executable, '-m', 'app.train_worker', log_type],
            cwd=cwd,
            env=train_env,
            stdout=log_f,
            stderr=subprocess.STDOUT if log_f else subprocess.DEVNULL,
            start_new_session=True,
        )
        if log_f:
            log_f.close()
        with training_lock:
            training_status[log_type]['pid'] = proc.pid
        logger.info(f"Started training subprocess for {log_type} (pid={proc.pid}, using all {max_cpus} CPUs)")
        return 'started'

    def _sync_training_status_from_subprocess(log_type):
        """If training was run in subprocess and has finished, read result file and update status."""
        with training_lock:
            st = training_status.get(log_type)
            if not st or st.get('status') != 'training' or st.get('pid') is None:
                return
            pid = st['pid']
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            pass
        except Exception:
            return
        else:
            return  # process still running
        result_done = os.path.join(TRAINING_RESULT_DIR, f'training_done_{log_type}.json')
        result_fail = os.path.join(TRAINING_RESULT_DIR, f'training_failed_{log_type}.json')
        result_data = None
        for path in (result_done, result_fail):
            if os.path.exists(path):
                try:
                    with open(path) as f:
                        result_data = json.load(f)
                    try:
                        os.unlink(path)
                    except OSError:
                        pass
                except Exception as e:
                    logger.warning("Failed to read training result %s: %s", path, e)
                break
        if result_data is None:
            return
        with training_lock:
            training_status[log_type] = {
                'status': result_data.get('status', 'error'),
                'result': result_data,
                'completed_at': datetime.now().isoformat(),
                'pid': None,
            }
            if result_data.get('status') == 'success':
                training_progress[log_type] = {'status': 'completed', 'progress_pct': 100}
                try:
                    models[log_type].load_latest_model()
                    logger.info("Reloaded model for %s after subprocess training", log_type)
                except Exception as e:
                    logger.warning("Reload model after training failed for %s: %s", log_type, e)
            else:
                training_progress[log_type] = {
                    'status': 'error',
                    'error': result_data.get('error', 'Unknown error'),
                }
            # Sequential training: start next in queue (one at a time, full CPU for each)
            next_type = training_pending_queue.popleft() if training_pending_queue else None
        if next_type is not None:
            logger.info("Starting next queued training: %s", next_type)
            _start_training_subprocess(next_type)

    class TrainingProgressCallback(Callback):
        """Callback to track training progress in real-time."""
        def __init__(self, log_type, total_epochs):
            super().__init__()
            self.log_type = log_type
            self.total_epochs = total_epochs
            self.start_time = None
            
        def on_train_begin(self, logs=None):
            self.start_time = datetime.now()
            with training_lock:
                training_progress[self.log_type] = {
                    'current_epoch': 0,
                    'total_epochs': self.total_epochs,
                    'loss': 0.0,
                    'val_loss': 0.0,
                    'progress_pct': 0,
                    'started_at': self.start_time.isoformat(),
                    'status': 'training'
                }
        
        def on_epoch_end(self, epoch, logs=None):
            logs = logs or {}
            elapsed = (datetime.now() - self.start_time).total_seconds()
            avg_time_per_epoch = elapsed / (epoch + 1)
            remaining_epochs = self.total_epochs - (epoch + 1)
            eta_seconds = avg_time_per_epoch * remaining_epochs
            
            with training_lock:
                training_progress[self.log_type] = {
                    'current_epoch': epoch + 1,
                    'total_epochs': self.total_epochs,
                    'loss': float(logs.get('loss', 0)),
                    'val_loss': float(logs.get('val_loss', 0)),
                    'progress_pct': int((epoch + 1) / self.total_epochs * 100),
                    'elapsed_seconds': int(elapsed),
                    'eta_seconds': int(eta_seconds),
                    'started_at': self.start_time.isoformat(),
                    'status': 'training'
                }
        
        def on_train_end(self, logs=None):
            with training_lock:
                if self.log_type in training_progress:
                    training_progress[self.log_type]['status'] = 'completed'
    
    def train_model_background(log_type):
        """Train the model in background thread to avoid blocking API."""
        try:
            with training_lock:
                training_status[log_type] = {'status': 'training', 'started_at': datetime.now().isoformat()}
                training_progress[log_type] = {'status': 'initializing', 'progress_pct': 0}
            
            logger.info(f"Starting background training for {log_type}...")
            result = train_model(log_type)
            
            with training_lock:
                training_status[log_type] = {
                    'status': result.get('status', 'error'),
                    'result': result,
                    'completed_at': datetime.now().isoformat()
                }
                if log_type in training_progress:
                    training_progress[log_type]['status'] = 'completed'
            
            logger.info(f"Background training completed for {log_type}: {result.get('status')}")
            
            # Reload model after successful training
            if result.get('status') == 'success':
                logger.info(f"Reloading model for {log_type} after training...")
                models[log_type].load_latest_model()
                
        except Exception as e:
            logger.error(f"Error in background training for {log_type}: {e}")
            with training_lock:
                training_status[log_type] = {
                    'status': 'error',
                    'error': str(e),
                    'completed_at': datetime.now().isoformat()
                }
                training_progress[log_type] = {'status': 'error', 'error': str(e)}
    
    def train_model(log_type):
        """Train the model using collected data for a specific log type."""
        data = load_collected_data(log_type)
        config = LOG_TYPES[log_type]
        data_info = {
            'status': 'error',
            'message': 'Insufficient data for training',
            'data_size': 0,
            'required_size': config['timesteps'],
            'total_rows': get_total_rows(log_type),
            'log_type': log_type
        }
        
        if data is None:
            return data_info
        
        # Ensure data is a 2D array
        if len(data.shape) == 1:
            data = data.reshape(1, -1)
        
        current_rows = data.shape[0]
        data_info['data_size'] = current_rows
        
        # Check if we have enough data for training
        if current_rows < config['timesteps']:
            data_info['message'] = f'Insufficient data for training {log_type}. Got {current_rows} rows, need at least {config["timesteps"]} rows.'
            return data_info
        
        try:
            # Update total rows before training
            total_rows = update_total_rows(current_rows, log_type)
            
            # Reshape data for LSTM (samples, timesteps, features)
            n_samples = len(data) - config['timesteps'] + 1
            
            logger.info(f"Creating {n_samples} training sequences for {log_type}...")
            X = np.zeros((n_samples, config['timesteps'], config['input_dim']), dtype=np.float32)
            for i in range(n_samples):
                X[i] = data[i:i + config['timesteps']]
            
            logger.info(f"Training data shape: {X.shape}, Memory: {X.nbytes / (1024**2):.2f} MB")
            
            # Create progress callback
            progress_callback = TrainingProgressCallback(log_type, total_epochs=100)
            
            # Train the model (matching train_model_01.py parameters)
            training_result = models[log_type].train(
                data=X,
                epochs=100,
                batch_size=64,  # Matches new-code
                validation_split=0.2,
                progress_callback=progress_callback
            )

            # Handle training failure
            if training_result.get('status') != 'success':
                data_info['message'] = training_result.get('message', str(training_result))
                return data_info

            # Save model info to database
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
                fpr=metrics.get('false_positive_rate')
            )

            # Clear collected data after successful training
            db.clear_collected_data(log_type)

            return {
                'status': 'success',
                'message': f'Model trained successfully for {log_type}',
                'model_path': training_result['model_path'],
                'timestamp': training_result['timestamp'],
                'data_size': current_rows,
                'total_rows': total_rows,
                'training_samples': n_samples,
                'log_type': log_type,
                'training_loss': training_result['training_loss'],
                'validation_loss': training_result['validation_loss'],
                'epochs_trained': training_result['epochs_trained'],
                'anomaly_threshold': training_result.get('anomaly_threshold'),
                'metrics': training_result.get('metrics', {})
            }
        except Exception as e:
            logger.error(f"Error training {log_type} model: {e}", exc_info=True)
            data_info['message'] = f'Error training {log_type} model: {str(e)}'
            return data_info
    
    # OLD PREDICT ENDPOINT REMOVED - Use /predict/<log_type> instead
    
    # OLD LEARNING ENABLE ENDPOINT REMOVED - Use /learning/enable/<log_type> instead
    
    # OLD LEARNING DISABLE ENDPOINT REMOVED - Use /learning/disable/<log_type> instead
    
    # OLD LEARNING STATUS ENDPOINT REMOVED - Use /learning/status/<log_type> instead
    
    @app.route('/models/status', methods=['GET'])
    def model_status():
        try:
            log_type = request.args.get('log_type')
            if log_type and log_type not in LOG_TYPES:
                return jsonify({
                    "status": "error",
                    "message": f"Invalid log type. Must be one of: {', '.join(LOG_TYPES.keys())}"
                }), 400
            
            model_path = os.environ.get('MODEL_PATH', '/app/models')
            if not os.path.exists(model_path):
                return jsonify({
                    "status": "error",
                    "message": f"Model directory not found at {model_path}"
                }), 404
            
            # Get model info for each log type
            model_info = {}
            for lt in LOG_TYPES.keys():
                if log_type and lt != log_type:
                    continue
                    
                lt_path = os.path.join(model_path, lt)
                if not os.path.exists(lt_path):
                    continue
                
                # List model files
                model_files = [f for f in os.listdir(lt_path) if (f.endswith('.h5') or f.endswith('.keras')) and '_tmp' not in f]
                model_files.sort(key=lambda x: os.path.getctime(os.path.join(lt_path, x)), reverse=True)
                
                # Get model information
                models_info = []
                for model_file in model_files:
                    full_path = os.path.join(lt_path, model_file)
                    created_time = datetime.fromtimestamp(os.path.getctime(full_path))
                    
                    # Get additional info from database
                    db_info = db.get_latest_model_info(lt)
                    
                    models_info.append({
                        'filename': model_file,
                        'created_at': created_time.isoformat(),
                        'size_bytes': os.path.getsize(full_path),
                        'training_info': {
                            'input_dim': db_info[1] if db_info else None,
                            'timesteps': db_info[2] if db_info else None,
                            'training_samples': db_info[4] if db_info else None,
                            'validation_loss': db_info[5] if db_info else None,
                            'training_loss': db_info[6] if db_info else None,
                            'anomaly_threshold': db_info[7] if db_info and len(db_info) > 7 else None,
                            'accuracy': db_info[8] if db_info and len(db_info) > 8 else None,
                            'precision': db_info[9] if db_info and len(db_info) > 9 else None,
                            'recall': db_info[10] if db_info and len(db_info) > 10 else None,
                            'f1_score': db_info[11] if db_info and len(db_info) > 11 else None,
                            'false_positive_rate': db_info[12] if db_info and len(db_info) > 12 else None
                        } if db_info else None
                    })
                
                model_info[lt] = {
                    'models': models_info,
                    'count': len(model_files),
                    'latest_model': models_info[0] if models_info else None,
                    'config': LOG_TYPES[lt]
                }
            
            return jsonify({
                "status": "success",
                "model_path": model_path,
                "log_types": model_info if not log_type else {log_type: model_info[log_type]}
            }), 200
        except Exception as e:
            return jsonify({
                "status": "error",
                "message": str(e)
            }), 500
    
    # Path-based endpoints (new supervised-style API)
    @app.route('/learning/status/<log_type>', methods=['GET'])
    def learning_status_path(log_type):
        """Learning status endpoint with log_type in URL path"""
        try:
            if not log_type or log_type not in LOG_TYPES:
                return jsonify({
                    "error": f"Invalid log type. Must be one of: {', '.join(LOG_TYPES.keys())}",
                    "status": "error"
                }), 400
            
            # Get learning status from database
            status = get_learning_status(log_type)
            # Get current data size (fast count query - don't load all data)
            data_size = db.get_collected_data_count(log_type)
            # Get total rows processed
            total_rows = get_total_rows(log_type)
            # Get last reported buffer size
            buffer_info = BUFFER_STATUS.get(log_type, {'buffer_size': 0, 'updated_at': None})
            logger.info(f"Learning status for {log_type}: {'enabled' if status else 'disabled'}, Current data points: {data_size}, Total rows processed: {total_rows}")
            
            return jsonify({
                "status": "success",
                "learning_enabled": status,
                "log_type": log_type,
                "data_size": data_size,
                "total_rows": total_rows,
                "buffer_size": buffer_info.get('buffer_size', 0),
                "buffer_updated_at": buffer_info.get('updated_at')
            }), 200
        except Exception as e:
            logger.error(f"Error getting learning status: {e}")
            return jsonify({
                "error": f"Failed to get learning status: {str(e)}",
                "status": "error"
            }), 500

    @app.route('/buffer/update/<log_type>', methods=['POST'])
    def buffer_update(log_type):
        """Receive buffer size metrics from Flink for display/monitoring."""
        try:
            if not log_type or log_type not in LOG_TYPES:
                return jsonify({
                    "error": f"Invalid log type. Must be one of: {', '.join(LOG_TYPES.keys())}",
                    "status": "error"
                }), 400

            payload = request.get_json(silent=True) or {}
            buffer_size = int(payload.get('buffer_size', 0))
            BUFFER_STATUS[log_type] = {
                'buffer_size': buffer_size,
                'updated_at': datetime.utcnow().isoformat() + 'Z'
            }
            logger.info(f"Buffer update for {log_type}: size={buffer_size}")
            return jsonify({
                "status": "success",
                "log_type": log_type,
                "buffer_size": buffer_size
            }), 200
        except Exception as e:
            logger.error(f"Error updating buffer size for {log_type}: {e}")
            return jsonify({
                "error": f"Failed to update buffer size: {str(e)}",
                "status": "error"
            }), 500

    @app.route('/predict/<log_type>', methods=['POST'])
    def predict_path(log_type):
        """Predict endpoint with log_type in URL path - OPTIMIZED for batch predictions"""
        try:
            if not log_type or log_type not in LOG_TYPES:
                return jsonify({
                    "error": f"Invalid log type. Must be one of: {', '.join(LOG_TYPES.keys())}",
                    "status": "error"
                }), 400
            
            data = request.get_json()
            if not data:
                return jsonify({
                    "error": "No data provided",
                    "status": "error"
                }), 400
            
            # Get features from request
            features = data.get('data', [])
            if not features:
                return jsonify({
                    "error": "No features provided",
                    "status": "error"
                }), 400
            
            # Check if learning mode is enabled for this log type
            is_learning_enabled = get_learning_status(log_type)

            if is_learning_enabled:
                # Convert features to numpy array before saving
                features_np = np.array(features)
                save_collected_data(features_np, log_type)
                return jsonify({
                    "status": "success",
                    "message": f"Data collected for {log_type}",
                    "learning_enabled": True,
                    "log_type": log_type
                }), 200
            
            # If learning mode is not enabled, proceed with prediction
            if models[log_type].model is None:
                # Return batch_size scores (0.0) so Flink gets expected count
                # features is a list - could be [[f1,f2,...], [f1,f2,...]] (batch) or [f1,f2,...] (single)
                # Convert to numpy to get shape safely
                try:
                    features_arr = np.array(features)
                    n = features_arr.shape[0] if len(features_arr.shape) == 2 else 1
                except Exception:
                    n = 1
                results = [{"mae": 0.0, "is_anomaly": False} for _ in range(n)]
                return jsonify({
                    "prediction": "no_model",
                    "batch_size": n,
                    "status": "warning",
                    "message": f"No trained model available for {log_type}",
                    "results": results
                }), 200
            
            # Get threshold - use adaptive threshold from model, fallback to database
            model_obj = models[log_type]
            if model_obj.adaptive is not None:
                # Use adaptive rolling threshold
                current_threshold = model_obj.adaptive.get_threshold()
                threshold_method = "adaptive_rolling"
            else:
                # Fallback to static threshold from database
                current_threshold = db.get_anomaly_threshold(log_type)
                if current_threshold is None:
                    current_threshold = 0.1
                threshold_method = "static_db"
            current_threshold = float(current_threshold)
            
            config = LOG_TYPES[log_type]
            
            # Convert to numpy array
            input_array = np.array(features, dtype=np.float32)
            
            # Validate input dimensions and detect batch vs single
            if len(input_array.shape) == 1:
                # Single sample: [f1, f2, f3, ...]
                input_array = input_array.reshape(1, -1)
            
            batch_size = input_array.shape[0]
            
            if input_array.shape[1] != config['input_dim']:
                return jsonify({
                    "error": f"Invalid input dimension. Expected {config['input_dim']}, got {input_array.shape[1]}",
                    "status": "error"
                }), 400
            
            # Scaling: use scaler params if available, else transform() (handles sklearn version / corrupted scaler)
            if models[log_type].scaler is not None:
                scaler = models[log_type].scaler
                # Check if scaler was fitted (has scale_ attribute)
                if hasattr(scaler, 'scale_') or hasattr(scaler, 'data_min_'):
                    data_min = getattr(scaler, 'data_min_', None)
                    if data_min is None:
                        data_min = getattr(scaler, 'data_min', None)
                    data_range = getattr(scaler, 'data_range_', None)
                    if data_min is not None and data_range is not None:
                        scaled_batch = (input_array - data_min) / (data_range + 1e-8)
                        scaled_batch = np.clip(scaled_batch, 0, 1)
                    elif hasattr(scaler, 'scale_'):
                        # Use scale_ and min_ from fitted scaler
                        scaled_batch = scaler.transform(input_array)
                    else:
                        logger.warning(f"Scaler not properly fitted for {log_type}, using raw data")
                        scaled_batch = input_array
                else:
                    logger.warning(f"Scaler not fitted for {log_type}, using raw data")
                    scaled_batch = input_array
            else:
                logger.warning(f"No scaler available for {log_type}, using raw data")
                scaled_batch = input_array
            
            # ULTRA-OPTIMIZED: Stateless batch prediction
            # Use NON-OVERLAPPING windows for real-time detection (not sliding window training approach)
            if batch_size >= config['timesteps']:
                # Split batch into non-overlapping sequences
                # Example: 100 samples, 10 timesteps = 10 sequences (not 91!)
                num_complete_sequences = batch_size // config['timesteps']
                usable_samples = num_complete_sequences * config['timesteps']
                
                if num_complete_sequences == 0:
                    # Fall back to sliding window for small batches
                    num_complete_sequences = 1
                    usable_samples = config['timesteps']
                
                # Reshape into non-overlapping sequences: (num_sequences, timesteps, features)
                sequences = scaled_batch[:usable_samples].reshape(
                    num_complete_sequences, 
                    config['timesteps'], 
                    config['input_dim']
                ).astype(np.float32)
                
                # FAST: Single TensorFlow call for entire batch
                reconstructions = models[log_type].model.predict(
                    sequences, 
                    verbose=0, 
                    batch_size=num_complete_sequences  # Predict all at once
                )
                
                # Calculate MAE for each sequence
                mae_scores = np.mean(np.abs(sequences - reconstructions), axis=(1, 2))
                
                # Feed MAE scores to adaptive threshold
                if model_obj.adaptive is not None:
                    model_obj.adaptive.add_mae_batch(mae_scores.tolist())
                
                # Expand scores to match input batch size (each sequence covers 'timesteps' samples)
                expanded_scores = []
                for mae in mae_scores:
                    # Each sequence's score applies to all samples in that sequence
                    expanded_scores.extend([mae] * config['timesteps'])
                
                # Pad if needed (unused samples at end)
                while len(expanded_scores) < batch_size:
                    expanded_scores.append(mae_scores[-1] if len(mae_scores) > 0 else 0.0)
                
                # Trim to exact batch size
                expanded_scores = expanded_scores[:batch_size]
                
                # Return results
                results = [
                    {
                        "mae": float(score),
                        "is_anomaly": bool(score >= current_threshold)
                    }
                    for score in expanded_scores
                ]
                
                return jsonify({
                    "status": "success",
                    "batch_size": len(results),
                    "num_sequences": num_complete_sequences,
                    "threshold": float(current_threshold),
                    "threshold_method": threshold_method,
                    "results": results
                }), 200
            
            else:
                # batch_size < timesteps: use sliding window, return 1 score per sample (Flink expects batch_size scores)
                for row in scaled_batch:
                    time_windows[log_type].append(row)
                
                if len(time_windows[log_type]) < config['timesteps']:
                    # Buffering: replicate 0.0 so Flink gets batch_size scores
                    results = [{"mae": 0.0, "is_anomaly": False} for _ in range(batch_size)]
                    return jsonify({
                        "prediction": "buffering",
                        "batch_size": batch_size,
                        "threshold": float(current_threshold),
                        "threshold_method": threshold_method,
                        "status": "success",
                        "message": f"Buffering: {len(time_windows[log_type])}/{config['timesteps']}",
                        "results": results
                    }), 200
                
                # Create sequence from window, get one score, replicate for each sample in batch
                time_window = np.array(list(time_windows[log_type])[-config['timesteps']:], dtype=np.float32)
                time_window = time_window.reshape(1, config['timesteps'], config['input_dim'])
                
                reconstruction = models[log_type].model.predict(time_window, verbose=0)
                mae = float(np.mean(np.abs(time_window - reconstruction)))
                
                # Feed MAE to adaptive threshold
                if model_obj.adaptive is not None:
                    model_obj.adaptive.add_mae(mae)
                
                # Return batch_size scores (Flink expects 1 per input sample)
                results = [
                    {"mae": mae, "is_anomaly": bool(mae >= current_threshold)}
                    for _ in range(batch_size)
                ]
                return jsonify({
                    "prediction": "anomaly" if mae >= current_threshold else "normal",
                    "batch_size": batch_size,
                    "threshold": float(current_threshold),
                    "threshold_method": threshold_method,
                    "status": "success",
                    "results": results
                }), 200
            
        except Exception as e:
            import traceback
            logger.error(f"Error in prediction: {e}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            return jsonify({
                "error": f"Prediction failed: {str(e)}",
                "status": "error"
            }), 500

    @app.route('/learning/enable/<log_type>', methods=['POST'])
    def enable_learning_path(log_type):
        """Enable learning endpoint with log_type in URL path"""
        try:
            if not log_type or log_type not in LOG_TYPES:
                return jsonify({
                    "error": f"Invalid log type. Must be one of: {', '.join(LOG_TYPES.keys())}",
                    "status": "error"
                }), 400
            
            set_learning_status(True, log_type)
            data_size = db.get_collected_data_count(log_type)
            logger.info(f"Learning enabled for {log_type} with {data_size} existing data points")
            
            return jsonify({
                "status": "success",
                "message": f"Learning mode enabled for {log_type}",
                "learning_enabled": True,
                "log_type": log_type,
                "current_data_size": data_size
            }), 200
        except Exception as e:
            logger.error(f"Error enabling learning mode: {e}")
            return jsonify({
                "error": f"Failed to enable learning mode: {str(e)}",
                "status": "error"
            }), 500

    @app.route('/learning/disable/<log_type>', methods=['POST'])
    def disable_learning_path(log_type):
        """Disable learning endpoint with log_type in URL path - starts training in background (subprocess)"""
        try:
            if not log_type or log_type not in LOG_TYPES:
                return jsonify({
                    "error": f"Invalid log type. Must be one of: {', '.join(LOG_TYPES.keys())}",
                    "status": "error"
                }), 400
            _sync_training_status_from_subprocess(log_type)
            # Check if already training
            with training_lock:
                if log_type in training_status and training_status[log_type].get('status') == 'training':
                    return jsonify({
                        "status": "info",
                        "message": f"Training already in progress for {log_type}",
                        "training_status": training_status[log_type]
                    }), 200
            
            # Disable learning mode immediately
            set_learning_status(False, log_type)
            
            # Get data count quickly (don't load full data yet - do that in background)
            data_count = db.get_collected_data_count(log_type)
            logger.info(f"Disabling learning mode for {log_type} with {data_count} collected data points")
            
            # Sequential training: one log type at a time, full CPU for active job (queue if one running)
            if data_count >= LOG_TYPES[log_type]['timesteps']:
                outcome = _start_training_subprocess(log_type)
                if outcome == 'started':
                    with training_lock:
                        qlen = len(training_pending_queue)
                    response = {
                        "status": "success",
                        "message": f"Learning mode disabled for {log_type}. Training started (one job at a time, full CPU).",
                        "learning_enabled": False,
                        "log_type": log_type,
                        "final_data_size": data_count,
                        "training_started": True,
                        "queue_length": qlen,
                    }
                elif outcome == 'queued':
                    with training_lock:
                        qlen = len(training_pending_queue)
                    response = {
                        "status": "success",
                        "message": f"Learning mode disabled for {log_type}. Training queued (another job running; {qlen} ahead in queue).",
                        "learning_enabled": False,
                        "log_type": log_type,
                        "final_data_size": data_count,
                        "training_started": False,
                        "queued": True,
                        "queue_length": qlen,
                    }
                else:
                    # outcome could be 'insufficient_data' or other - log it for debugging
                    logger.warning(f"Training not started for {log_type}, outcome={outcome}, data_count={data_count}")
                    response = {
                        "status": "success",
                        "message": f"Learning mode disabled for {log_type}. Training not started (outcome: {outcome}, data: {data_count}).",
                        "learning_enabled": False,
                        "log_type": log_type,
                        "final_data_size": data_count,
                        "required_rows": LOG_TYPES[log_type]['timesteps'],
                        "training_started": False,
                        "debug_outcome": outcome  # Added for debugging
                    }
            else:
                required = LOG_TYPES[log_type]['timesteps']
                response = {
                    "status": "success",
                    "message": f"Learning mode disabled for {log_type}. Insufficient data for training (have {data_count}, need at least {required} rows).",
                    "learning_enabled": False,
                    "log_type": log_type,
                    "final_data_size": data_count,
                    "required_rows": required,
                    "training_started": False
                }
            
            return jsonify(response), 200
        except Exception as e:
            logger.error(f"Error disabling learning mode: {e}", exc_info=True)
            return jsonify({
                "error": f"Failed to disable learning mode: {str(e)}",
                "status": "error"
            }), 500

    @app.route('/models/reload/<log_type>', methods=['POST'])
    def reload_model(log_type):
        """Reload the latest model for a specific log type without restarting the container"""
        try:
            if not log_type or log_type not in LOG_TYPES:
                return jsonify({
                    "error": f"Invalid log type. Must be one of: {', '.join(LOG_TYPES.keys())}",
                    "status": "error"
                }), 400
            
            logger.info(f"Reloading model for {log_type}...")
            model_path = models[log_type].load_latest_model()
            
            if model_path:
                logger.info(f"Successfully reloaded model for {log_type}: {model_path}")
                return jsonify({
                    "status": "success",
                    "message": f"Model reloaded successfully for {log_type}",
                    "log_type": log_type,
                    "model_path": model_path,
                    "model_loaded": True
                }), 200
            else:
                logger.warning(f"No model found to reload for {log_type}")
                return jsonify({
                    "status": "warning",
                    "message": f"No trained model available for {log_type}",
                    "log_type": log_type,
                    "model_loaded": False
                }), 200
        except Exception as e:
            logger.error(f"Error reloading model for {log_type}: {e}")
            return jsonify({
                "error": f"Failed to reload model: {str(e)}",
                "status": "error"
            }), 500

    @app.route('/models/reload', methods=['POST'])
    def reload_all_models():
        """Reload all models without restarting the container"""
        try:
            results = {}
            for log_type in LOG_TYPES.keys():
                logger.info(f"Reloading model for {log_type}...")
                model_path = models[log_type].load_latest_model()
                results[log_type] = {
                    "model_loaded": model_path is not None,
                    "model_path": model_path
                }
            
            all_loaded = all(result["model_loaded"] for result in results.values())
            
            return jsonify({
                "status": "success",
                "message": "Model reload completed",
                "all_models_loaded": all_loaded,
                "results": results
            }), 200
        except Exception as e:
            logger.error(f"Error reloading all models: {e}")
            return jsonify({
                "error": f"Failed to reload models: {str(e)}",
                "status": "error"
            }), 500

    @app.route('/threshold/<log_type>', methods=['GET'])
    def get_threshold(log_type):
        """Get the anomaly threshold for a specific log type"""
        try:
            if not log_type or log_type not in LOG_TYPES:
                return jsonify({
                    "error": f"Invalid log type. Must be one of: {', '.join(LOG_TYPES.keys())}",
                    "status": "error"
                }), 400
            
            # Get threshold from database
            threshold = db.get_anomaly_threshold(log_type)
            
            if threshold is None:
                return jsonify({
                    "status": "warning",
                    "message": f"No threshold found for {log_type}. Model may not be trained yet.",
                    "log_type": log_type,
                    "threshold": None,
                    "has_threshold": False
                }), 200
            
            return jsonify({
                "status": "success",
                "log_type": log_type,
                "threshold": threshold,
                "has_threshold": True
            }), 200
            
        except Exception as e:
            logger.error(f"Error getting threshold for {log_type}: {e}")
            return jsonify({
                "error": f"Failed to get threshold: {str(e)}",
                "status": "error"
            }), 500

    @app.route('/threshold/<log_type>', methods=['PUT', 'POST'])
    def set_threshold(log_type):
        """Set the anomaly threshold manually for a specific log type"""
        try:
            if not log_type or log_type not in LOG_TYPES:
                return jsonify({
                    "error": f"Invalid log type. Must be one of: {', '.join(LOG_TYPES.keys())}",
                    "status": "error"
                }), 400
            
            data = request.get_json()
            if not data or 'threshold' not in data:
                return jsonify({
                    "error": "Missing 'threshold' in request body",
                    "status": "error"
                }), 400
            
            threshold = float(data['threshold'])
            if threshold < 0:
                return jsonify({
                    "error": "Threshold must be a positive number",
                    "status": "error"
                }), 400
            
            # Update threshold in model_info table
            success = db.update_threshold(log_type, threshold)
            
            if success:
                logger.info(f"Threshold manually set to {threshold} for {log_type}")
                return jsonify({
                    "status": "success",
                    "log_type": log_type,
                    "threshold": threshold,
                    "message": f"Threshold updated to {threshold}"
                }), 200
            else:
                return jsonify({
                    "status": "error",
                    "error": "Failed to update threshold"
                }), 500
            
        except Exception as e:
            logger.error(f"Error setting threshold for {log_type}: {e}")
            return jsonify({
                "error": f"Failed to set threshold: {str(e)}",
                "status": "error"
            }), 500

    @app.route('/adaptive/<log_type>', methods=['GET'])
    def get_adaptive_threshold(log_type):
        """Get adaptive threshold statistics for a specific log type"""
        try:
            if not log_type or log_type not in LOG_TYPES:
                return jsonify({
                    "error": f"Invalid log type. Must be one of: {', '.join(LOG_TYPES.keys())}",
                    "status": "error"
                }), 400
            
            model_obj = models[log_type]
            if model_obj.adaptive is None:
                return jsonify({
                    "status": "warning",
                    "message": "Adaptive threshold not enabled for this log type",
                    "log_type": log_type,
                    "adaptive_enabled": False
                }), 200
            
            stats = model_obj.get_adaptive_stats()
            
            return jsonify({
                "status": "success",
                "log_type": log_type,
                "adaptive_enabled": True,
                "statistics": stats
            }), 200
            
        except Exception as e:
            logger.error(f"Error getting adaptive threshold stats: {e}")
            return jsonify({
                "error": f"Failed to get adaptive threshold stats: {str(e)}",
                "status": "error"
            }), 500

    @app.route('/training/status/<log_type>', methods=['GET'])
    def get_training_status(log_type):
        """Get the current training status for a specific log type"""
        try:
            if not log_type or log_type not in LOG_TYPES:
                return jsonify({
                    "error": f"Invalid log type. Must be one of: {', '.join(LOG_TYPES.keys())}",
                    "status": "error"
                }), 400
            _sync_training_status_from_subprocess(log_type)
            with training_lock:
                status = training_status.get(log_type, {'status': 'idle', 'message': 'No training in progress'})
            
            return jsonify({
                "status": "success",
                "log_type": log_type,
                "training_status": status
            }), 200
            
        except Exception as e:
            logger.error(f"Error getting training status for {log_type}: {e}")
            return jsonify({
                "error": f"Failed to get training status: {str(e)}",
                "status": "error"
            }), 500

    @app.route('/training/progress/<log_type>', methods=['GET'])
    def get_training_progress(log_type):
        """Get real-time training progress for a specific log type"""
        try:
            if not log_type or log_type not in LOG_TYPES:
                return jsonify({
                    "error": f"Invalid log type. Must be one of: {', '.join(LOG_TYPES.keys())}",
                    "status": "error"
                }), 400
            _sync_training_status_from_subprocess(log_type)
            with training_lock:
                progress = training_progress.get(log_type)
                status = training_status.get(log_type)
            
            if progress is None:
                return jsonify({
                    "status": "success",
                    "log_type": log_type,
                    "training_active": False,
                    "message": "No training in progress"
                }), 200
            
            return jsonify({
                "status": "success",
                "log_type": log_type,
                "training_active": progress.get('status') == 'training',
                "progress": progress,
                "training_status": status
            }), 200
            
        except Exception as e:
            logger.error(f"Error getting training progress for {log_type}: {e}")
            return jsonify({
                "error": f"Failed to get training progress: {str(e)}",
                "status": "error"
            }), 500

    @app.route('/metrics/<log_type>', methods=['GET'])
    def get_metrics(log_type):
        """Get model performance metrics for a specific log type"""
        try:
            if not log_type or log_type not in LOG_TYPES:
                return jsonify({
                    "error": f"Invalid log type. Must be one of: {', '.join(LOG_TYPES.keys())}",
                    "status": "error"
                }), 400
            
            # Get model info from database
            db_info = db.get_latest_model_info(log_type)
            
            if db_info is None:
                return jsonify({
                    "status": "warning",
                    "message": f"No trained model found for {log_type}",
                    "log_type": log_type,
                    "has_metrics": False
                }), 200
            
            # Extract metrics
            metrics = {
                'log_type': log_type,
                'model_path': db_info[0],
                'created_at': db_info[3].isoformat() if db_info[3] else None,
                'training_samples': db_info[4],
                'validation_loss': db_info[5],
                'training_loss': db_info[6],
                'anomaly_threshold': db_info[7] if len(db_info) > 7 else None,
                'accuracy': db_info[8] if len(db_info) > 8 else None,
                'precision': db_info[9] if len(db_info) > 9 else None,
                'recall': db_info[10] if len(db_info) > 10 else None,
                'f1_score': db_info[11] if len(db_info) > 11 else None,
                'false_positive_rate': db_info[12] if len(db_info) > 12 else None
            }
            
            return jsonify({
                "status": "success",
                "log_type": log_type,
                "has_metrics": True,
                "metrics": metrics
            }), 200
            
        except Exception as e:
            logger.error(f"Error getting metrics for {log_type}: {e}")
            return jsonify({
                "error": f"Failed to get metrics: {str(e)}",
                "status": "error"
            }), 500

    return app 