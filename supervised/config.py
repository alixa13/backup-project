"""
Configuration file for Elite IDS Model
Adjust these parameters to tune model performance
"""

# ==============================================================================
# FEATURE ENGINEERING
# ==============================================================================

FEATURE_CONFIG = {
    # Features to apply log transformation (reduces variance)
    'log_transform_features': [
        'sbytes', 'dbytes', 'sload', 'dload', 'dur', 'sinpkt', 'dinpkt'
    ],
    
    # Number of features to select via Elastic Net
    # Research shows 24 is optimal for UNSW-NB15
    # Range: 15-30 features
    'n_selected_features': 24,
    
    # Elastic Net parameters for feature selection
    'elastic_net_alpha': 0.01,  # Regularization strength (lower = more features)
    'elastic_net_l1_ratio': 0.5,  # Balance L1/L2 (0.5 = equal balance)
}

# ==============================================================================
# CLASS IMBALANCE HANDLING
# ==============================================================================

CLASS_WEIGHT_CONFIG = {
    # Weight strategy: 'extreme', 'balanced', or 'custom'
    'strategy': 'extreme',
    
    # Extreme strategy weights (for rare classes)
    'extreme_weights': {
        'rare_threshold': 1000,      # Classes with < 1000 samples
        'rare_weight': 50.0,          # Weight for rare classes (1:50 ratio)
        'medium_threshold': 5000,     # Classes with 1000-5000 samples
        'medium_weight': 10.0,        # Weight for medium classes
        'common_weight': 1.0,         # Weight for common classes
    },
    
    # Custom weights (if strategy='custom')
    'custom_weights': {
        'Analysis': 50.0,
        'Backdoor': 50.0,
        'DoS': 10.0,
        'Exploits': 5.0,
        'Fuzzer': 10.0,
        'Generic': 1.0,
        'Normal': 1.0,
        'Reconnaissance': 5.0,
        'Shellcode': 50.0,
        'Worms': 50.0,
    }
}

# ==============================================================================
# MAIN CLASSIFIER (Cost-Sensitive XGBoost)
# ==============================================================================

MAIN_CLASSIFIER_CONFIG = {
    'n_estimators': 1000,          # Number of trees (higher = better but slower)
    'learning_rate': 0.03,         # Learning rate (0.01-0.1)
    'max_depth': 8,                # Tree depth (6-10 recommended)
    'min_child_weight': 5,         # Minimum samples per leaf (higher = less overfit)
    'subsample': 0.8,              # Row sampling per tree (0.7-0.9)
    'colsample_bytree': 0.8,       # Column sampling per tree (0.7-0.9)
    'reg_alpha': 0.1,              # L1 regularization (0-1)
    'reg_lambda': 5,               # L2 regularization (1-10)
    'tree_method': 'hist',         # 'hist' for speed, 'exact' for accuracy
    'n_jobs': -1,                  # Use all CPU cores
}

# ==============================================================================
# OVERLAP SPECIALIST (Balanced Bagging + Random Forest)
# ==============================================================================

OVERLAP_SPECIALIST_CONFIG = {
    # Random Forest base estimator
    'rf_n_estimators': 200,        # Trees per RF (100-300)
    'rf_max_depth': 15,            # Tree depth (10-20)
    'rf_min_samples_split': 5,     # Min samples to split (2-10)
    'rf_min_samples_leaf': 2,      # Min samples per leaf (1-5)
    'rf_max_features': 'sqrt',     # Features per split ('sqrt' or 'log2')
    
    # Balanced Bagging parameters
    'n_bagging_estimators': 10,    # Number of bagged estimators (5-15)
    'sampling_strategy': 'all',    # Balance all classes
    'replacement': False,          # Sample without replacement
}

# ==============================================================================
# OVERLAP DETECTION
# ==============================================================================

OVERLAP_CONFIG = {
    # Mahalanobis distance threshold for overlap detection
    # Higher = stricter (fewer samples marked as overlap)
    # Lower = looser (more samples marked as overlap)
    'mahalanobis_threshold': 2.5,  # Range: 2.0-4.0
    
    # Overlap detector XGBoost parameters
    'detector_n_estimators': 300,
    'detector_max_depth': 5,
    'detector_learning_rate': 0.05,
}

# ==============================================================================
# THRESHOLD OPTIMIZATION
# ==============================================================================

THRESHOLD_CONFIG = {
    # Minority class threshold adjustment
    # Values < 1.0 make model more aggressive for minorities
    # Values > 1.0 make model more conservative
    'minority_threshold_multiplier': 0.7,  # Range: 0.5-1.0
    
    # Classes considered minorities for threshold adjustment
    'minority_classes': [
        'Analysis', 'Backdoor', 'Worms', 'Shellcode'
    ],
    
    # Default threshold if optimization fails
    'default_threshold': 0.5,
}

# ==============================================================================
# TRAINING PARAMETERS
# ==============================================================================

TRAINING_CONFIG = {
    # Train/test split
    'test_size': 0.2,
    'random_state': 42,
    'stratify': True,  # Maintain class distribution in split
    
    # Model saving
    'save_threshold_f1': 0.85,  # Only save if macro F1 >= this value
    'model_dir': './models',
    'auto_save': True,
}

# ==============================================================================
# PERFORMANCE TUNING GUIDE
# ==============================================================================

"""
IF YOUR MACRO F1 IS:

< 60%:
- Problem: Basic model failing
- Fix: Check data quality, increase n_estimators to 1500, increase rare_weight to 100

60-70%:
- Problem: Class imbalance not fully addressed
- Fix: Increase rare_weight to 70-100, adjust minority_threshold_multiplier to 0.5-0.6

70-80%:
- Problem: Class overlap issues
- Fix: Lower mahalanobis_threshold to 2.0, increase overlap specialist n_bagging_estimators to 15

80-85%:
- Problem: Need fine-tuning
- Fix: Adjust n_selected_features (try 20 or 28), tune main_classifier max_depth (try 10)

85-90%:
- Problem: Close but not quite there
- Fix: Tune threshold_multiplier precisely (try 0.65, 0.75), increase n_estimators to 1200

> 90%:
- Congrats! You've achieved research-level performance
- Consider: ensemble with multiple models, test on other datasets

SPECIFIC CLASS PROBLEMS:

Analysis/Backdoor F1 < 50%:
- Increase their custom_weights to 80-100
- Lower minority_threshold_multiplier to 0.5
- Increase overlap specialist n_bagging_estimators

DoS F1 < 60%:
- DoS has high within-class variance
- Increase main_classifier max_depth to 10
- Consider adding DoS to minority_classes list

Generic/Normal being confused:
- These overlap significantly
- Lower mahalanobis_threshold to 2.0
- Increase rf_max_depth to 20

Worms/Shellcode (very rare):
- Extreme imbalance issue
- Set their weights to 100+
- Consider generating synthetic samples (carefully)
"""
