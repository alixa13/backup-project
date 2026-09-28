# The throwaway training image for the S7comm detector v2 (spec section 6): CPU-only
# PyTorch and the scientific stack, nothing else. Built and run on server3 only.
FROM python:3.12-slim
RUN pip install --no-cache-dir torch==2.5.1 --index-url https://download.pytorch.org/whl/cpu \
 && pip install --no-cache-dir "numpy>=1.26" "pandas>=2.2" "scikit-learn>=1.5" "joblib>=1.4" \
      "onnx>=1.16" "onnxruntime>=1.19" "pyyaml>=6.0" pytest
ENV PYTHONPATH=/work/src PYTHONUNBUFFERED=1
WORKDIR /work
