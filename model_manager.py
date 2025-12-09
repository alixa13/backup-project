from typing import Union, List, Tuple

def predict(log_type: str, features: Union[List[str], List[List[str]]]) -> Tuple[str, float]:
    """
    Predict using the appropriate model.
    
    Args:
        log_type: Type of log ('conn', 'ssl', 'dns', 'http')
        features: List of features or list of lists of features
    
    Returns:
        Tuple of (predicted_label, anomaly_score)
    """
    if log_type not in MODELS:
        msg = f"Unsupported log_type: {log_type}"
        logger.error(msg)
        raise ValueError(msg)
    
    clf = MODELS[log_type]
    encoder = ENCODERS[log_type]

    # Handle empty features case
    if not features:
        logger.warning(f"Empty features received for {log_type}")
        return "unknown", 0.0
        
    # Handle both 1D and 2D input formats
    if len(features) > 0 and isinstance(features[0], list):
        # If 2D array, take first row
        features = features[0]
    
    logger.info(f"Received prediction request for {log_type} with {len(features)} features") 