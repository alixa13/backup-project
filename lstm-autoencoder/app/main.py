"""
Main Flask application for the LSTM Autoencoder API.
"""
from flask import Flask, request, jsonify
import os
import json
import numpy as np
from datetime import datetime
import logging
from logging.handlers import RotatingFileHandler
from .model import LSTMAutoencoder
from .database import Database

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
LOG_TYPES = {
    'http': {
        'input_dim': 28,  # Number of features for HTTP logs
        'timesteps': 24,  # Number of time steps
        'encoding_dim': 64
    },
    'ssl': {
        'input_dim': 19,  # Number of features for SSL logs
        'timesteps': 24,
        'encoding_dim': 64
    },
    'dns': {
        'input_dim': 24,  # Number of features for DNS logs
        'timesteps': 24,
        'encoding_dim': 64
    },
    'conn': {
        'input_dim': 20,  # Number of features for connection logs
        'timesteps': 24,
        'encoding_dim': 64
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
            X = np.zeros((n_samples, config['timesteps'], config['input_dim']))
            for i in range(n_samples):
                X[i] = data[i:i + config['timesteps']]
            
            # Train the model
            training_result = models[log_type].train(
                data=X,
                epochs=5,
                batch_size=256,
                validation_split=0.1
            )
            
            # Save model info to database
            db.save_model_info(
                log_type=log_type,
                model_path=training_result['model_path'],
                input_dim=config['input_dim'],
                timesteps=config['timesteps'],
                training_samples=n_samples,
                validation_loss=training_result['validation_loss'],
                training_loss=training_result['training_loss']
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
                'epochs_trained': training_result['epochs_trained']
            }
        except Exception as e:
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
                model_files = [f for f in os.listdir(lt_path) if f.endswith('.h5')]
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
                            'training_loss': db_info[6] if db_info else None
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
        """Predict endpoint with log_type in URL path"""
        try:
            if not log_type or log_type not in LOG_TYPES:
                return jsonify({
                    "error": f"Invalid log type. Must be one of: {', '.join(LOG_TYPES.keys())}",
                    "status": "error"
                }), 400
            
            data = request.get_json()
            logger.debug(f"Received raw data for prediction: {data}")
            if not data:
                return jsonify({
                    "error": "No data provided",
                    "status": "error"
                }), 400
            
            # Get features from request (no log_type needed in JSON anymore)
            features = data.get('data', [])
            logger.debug(f"Extracted features: {features}")
            if not features:
                return jsonify({
                    "error": "No features provided",
                    "status": "error"
                }), 400
            
            # Check if learning mode is enabled for this log type
            is_learning_enabled = get_learning_status(log_type)
            logger.debug(f"Learning mode enabled for {log_type}: {is_learning_enabled}")

            if is_learning_enabled:
                logger.info(f"Learning mode enabled for {log_type}, collecting data...")
                try:
                    # Convert features to numpy array before saving
                    features_np = np.array(features)
                    save_collected_data(features_np, log_type)
                    return jsonify({
                        "status": "success",
                        "message": f"Data collected for {log_type}",
                        "learning_enabled": True,
                        "log_type": log_type
                    }), 200
                except Exception as e:
                    logger.error(f"Error saving collected data for {log_type}: {e}")
                    return jsonify({
                        "error": f"Failed to collect data for {log_type}: {str(e)}",
                        "status": "error"
                    }), 500
            
            # If learning mode is not enabled, proceed with prediction if a model is loaded
            if models[log_type].model is None:
                logger.warning(f"No trained model available for {log_type}. Skipping prediction.")
                return jsonify({
                    "prediction": "no_model",
                    "anomaly_score": 0.0,
                    "status": "warning",
                    "message": f"No trained model available for {log_type}"
                }), 200
            
            # Convert features to numpy array
            input_data = np.array(features, dtype=np.float64)
            config = LOG_TYPES[log_type]
            
            # Validate input dimensions
            if input_data.shape[-1] != config['input_dim']:
                return jsonify({
                    "error": f"Invalid input dimension for {log_type}. Expected {config['input_dim']}, got {input_data.shape[-1]}",
                    "status": "error"
                }), 400
            
            # Reshape for prediction
            input_data = input_data.reshape(1, 1, config['input_dim'])
            
            # Make prediction
            reconstruction = models[log_type].model.predict(input_data, verbose=0)
            mse = np.mean((input_data - reconstruction) ** 2)
            
            return jsonify({
                "prediction": "normal" if mse < 0.5 else "anomaly",
                "anomaly_scores": {"mse": float(mse)},
                "status": "success"
            }), 200
            
        except Exception as e:
            logger.error(f"Error in prediction: {e}")
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
        """Disable learning endpoint with log_type in URL path"""
        try:
            if not log_type or log_type not in LOG_TYPES:
                return jsonify({
                    "error": f"Invalid log type. Must be one of: {', '.join(LOG_TYPES.keys())}",
                    "status": "error"
                }), 400
            
            # Get current data size before disabling
            collected_data = load_collected_data(log_type)
            data_size = collected_data.shape[0] if collected_data is not None else 0
            logger.info(f"Disabling learning mode for {log_type} with {data_size} collected data points")
            
            # Train model if we have collected data
            training_result = train_model(log_type)
            
            # Disable learning mode
            set_learning_status(False, log_type)
            
            response = {
                "status": "success",
                "message": f"Learning mode disabled for {log_type}",
                "learning_enabled": False,
                "log_type": log_type,
                "final_data_size": data_size
            }
            
            if training_result.get('status') == 'success':
                logger.info(f"Training completed successfully for {log_type}: {json.dumps(training_result)}")
                response['training_result'] = training_result
            else:
                logger.warning(f"Training not performed or failed for {log_type}: {json.dumps(training_result)}")
            
            return jsonify(response), 200
        except Exception as e:
            logger.error(f"Error disabling learning mode: {e}")
            return jsonify({
                "error": f"Failed to disable learning mode: {str(e)}",
                "status": "error"
            }), 500

    return app 