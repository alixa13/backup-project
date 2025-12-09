"""
LSTM Autoencoder model for anomaly detection.
"""
import os
import json
import numpy as np
import tensorflow as tf
from tensorflow.keras.models import Sequential, load_model
from tensorflow.keras.layers import LSTM, Dense, Dropout
from tensorflow.keras.callbacks import EarlyStopping, ModelCheckpoint
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class LSTMAutoencoder:
    def __init__(self, log_type, input_dim, timesteps, encoding_dim=64):
        """
        Initialize LSTM Autoencoder model.
        
        Args:
            log_type (str): Type of log being processed (http, ssl, dns, conn)
            input_dim (int): Number of input features
            timesteps (int): Number of time steps for LSTM
            encoding_dim (int): Dimension of the encoded representation
        """
        self.log_type = log_type
        self.input_dim = input_dim
        self.timesteps = timesteps
        self.encoding_dim = encoding_dim
        self.model = None
        self.model_path = None
        
        # Set up model directory
        self.model_dir = os.path.join(os.environ.get('MODEL_PATH', '/app/models'), log_type)
        os.makedirs(self.model_dir, exist_ok=True)
        
        # Load latest model if available
        self.load_latest_model()
    
    def _build_model(self):
        """Build the LSTM Autoencoder model architecture."""
        model = Sequential([
            # Encoder
            LSTM(self.encoding_dim, activation='relu', input_shape=(self.timesteps, self.input_dim),
                 return_sequences=False),
            Dropout(0.2),
            
            # Decoder
            Dense(self.timesteps * self.encoding_dim, activation='relu'),
            Dropout(0.2),
            Dense(self.timesteps * self.input_dim, activation='sigmoid'),
            tf.keras.layers.Reshape((self.timesteps, self.input_dim))
        ])
        
        model.compile(
            optimizer='adam',
            loss='mse',
            metrics=['mae']
        )
        
        return model
    
    def train(self, data, epochs=100, batch_size=32, validation_split=0.2):
        """
        Train the LSTM Autoencoder model.
        
        Args:
            data (np.ndarray): Training data of shape (samples, timesteps, features)
            epochs (int): Number of training epochs
            batch_size (int): Batch size for training
            validation_split (float): Fraction of data to use for validation
        
        Returns:
            dict: Training results including model path and metrics
        """
        try:
            # Create new model
            self.model = self._build_model()
            
            # Generate timestamp for model versioning
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            model_filename = f'lstm_autoencoder_{self.log_type}_{timestamp}.h5'
            model_path = os.path.join(self.model_dir, model_filename)
            
            # Set up callbacks
            callbacks = [
                EarlyStopping(
                    monitor='val_loss',
                    patience=10,
                    restore_best_weights=True
                ),
                ModelCheckpoint(
                    model_path,
                    monitor='val_loss',
                    save_best_only=True
                )
            ]
            
            # Train model
            history = self.model.fit(
                data, data,  # Autoencoder: input = target
                epochs=epochs,
                batch_size=batch_size,
                validation_split=validation_split,
                callbacks=callbacks,
                verbose=1
            )
            
            # Save final model
            self.model.save(model_path)
            self.model_path = model_path
            
            # Get best validation loss
            best_epoch = np.argmin(history.history['val_loss'])
            validation_loss = history.history['val_loss'][best_epoch]
            training_loss = history.history['loss'][best_epoch]
            
            logger.info(f"Model training completed for {self.log_type}")
            logger.info(f"Best validation loss: {validation_loss:.4f}")
            logger.info(f"Training loss: {training_loss:.4f}")
            
            return {
                'status': 'success',
                'model_path': model_path,
                'timestamp': timestamp,
                'validation_loss': float(validation_loss),
                'training_loss': float(training_loss),
                'epochs_trained': best_epoch + 1
            }
        
        except Exception as e:
            logger.error(f"Error training model for {self.log_type}: {e}")
            return {
                'status': 'error',
                'message': str(e)
            }
    
    def predict(self, data):
        """
        Make predictions using the trained model.
        
        Args:
            data (np.ndarray): Input data of shape (samples, timesteps, features)
        
        Returns:
            tuple: (predictions, mse_scores, mae_scores)
        """
        if self.model is None:
            raise ValueError(f"No model loaded for {self.log_type}")
        
        try:
            # Make predictions
            predictions = self.model.predict(data)
            
            # Calculate reconstruction error
            mse = np.mean(np.square(data - predictions), axis=(1, 2))
            mae = np.mean(np.abs(data - predictions), axis=(1, 2))
            
            return predictions, mse, mae
        
        except Exception as e:
            logger.error(f"Error making predictions for {self.log_type}: {e}")
            raise
    
    def load_latest_model(self):
        """
        Load the latest trained model for this log type.
        
        Returns:
            str: Path to the loaded model, or None if no model found
        """
        try:
            # List all model files
            model_files = [f for f in os.listdir(self.model_dir) if f.endswith('.h5')]
            if not model_files:
                logger.warning(f"No trained models found for {self.log_type}")
                return None
            
            # Sort by creation time (newest first)
            model_files.sort(key=lambda x: os.path.getctime(os.path.join(self.model_dir, x)),
                           reverse=True)
            
            # Load the latest model
            latest_model = model_files[0]
            model_path = os.path.join(self.model_dir, latest_model)
            
            self.model = load_model(model_path)
            self.model_path = model_path
            
            logger.info(f"Loaded latest model for {self.log_type}: {latest_model}")
            return model_path
        
        except Exception as e:
            logger.error(f"Error loading latest model for {self.log_type}: {e}")
            return None
    
    def get_model_info(self):
        """
        Get information about the current model.
        
        Returns:
            dict: Model information including architecture and parameters
        """
        if self.model is None:
            return None
        
        return {
            'log_type': self.log_type,
            'input_dim': self.input_dim,
            'timesteps': self.timesteps,
            'encoding_dim': self.encoding_dim,
            'model_path': self.model_path,
            'layers': [
                {
                    'name': layer.name,
                    'type': layer.__class__.__name__,
                    'output_shape': layer.output_shape
                }
                for layer in self.model.layers
            ]
        } 