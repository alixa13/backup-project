import joblib

# Load the label encoder
label_encoder = joblib.load('./label_encoder_conn.joblib')

# Inspect the classes
print("Classes:", label_encoder.classes_)