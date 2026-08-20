#!/usr/bin/env python3
"""
LSTM Training Troubleshooter - Run inside lstm-autoencoder container:
  docker exec -it lstm-autoencoder python3 /app/troubleshoot_train.py [conn|dns|http|ssl]

Tests: DB load, data shape, model build, scaler fit, and optionally a short training run.
"""
import os
import sys

def main():
    log_type = sys.argv[1] if len(sys.argv) > 1 else "conn"
    if log_type not in ("conn", "dns", "http", "ssl"):
        print(f"Usage: {sys.argv[0]} [conn|dns|http|ssl]")
        sys.exit(1)

    print("=" * 60)
    print(f"LSTM Training Troubleshooter - {log_type}")
    print("=" * 60)

    # 1. Database
    print("\n[1] Database connection...")
    try:
        from app.database import Database
        db = Database()
        ok = db.check_connection()
        print(f"    {'OK' if ok else 'FAIL'}")
        if not ok:
            sys.exit(1)
    except Exception as e:
        print(f"    FAIL: {e}")
        sys.exit(1)

    # 2. Load collected data
    print("\n[2] Load collected data...")
    try:
        data = db.load_collected_data(log_type)
        if data is None:
            print("    No data. Enable learning, collect data, then disable to train.")
            sys.exit(1)
        print(f"    Loaded: shape={data.shape}, dtype={data.dtype}")
        if data.shape[0] < 10:
            print(f"    Need 10+ rows for training, got {data.shape[0]}")
            sys.exit(1)
    except Exception as e:
        print(f"    FAIL: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    # 3. Reshape for LSTM
    timesteps = 10
    input_dim = 33
    n_samples = len(data) - timesteps + 1
    print(f"\n[3] Reshape for LSTM (timesteps={timesteps}, input_dim={input_dim})...")
    if n_samples < 1:
        print("    Not enough rows for sequences")
        sys.exit(1)
    X = __import__("numpy").zeros((n_samples, timesteps, input_dim), dtype="float32")
    for i in range(n_samples):
        X[i] = data[i : i + timesteps]
    print(f"    X.shape = {X.shape}")

    # 4. Model build
    print("\n[4] Build model...")
    try:
        from app.model import LSTMAutoencoder
        model_wrapper = LSTMAutoencoder(
            log_type=log_type,
            input_dim=input_dim,
            timesteps=timesteps,
            encoding_dim=16,
        )
        model_wrapper.model = model_wrapper._build_model()
        print("    Model built OK")
    except Exception as e:
        print(f"    FAIL: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    # 5. Short training (2 epochs)
    print("\n[5] Short training (2 epochs)...")
    try:
        result = model_wrapper.train(
            data=X[: min(500, len(X))],  # limit samples for speed
            epochs=2,
            batch_size=64,
            validation_split=0.2,
        )
        status = result.get("status", "?")
        msg = result.get("message", "")
        print(f"    status={status}")
        if status == "success":
            print(f"    model_path={result.get('model_path')}")
        else:
            print(f"    message={msg}")
    except Exception as e:
        print(f"    FAIL: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    print("\n" + "=" * 60)
    print("Troubleshooting complete.")
    print("=" * 60)


if __name__ == "__main__":
    main()
