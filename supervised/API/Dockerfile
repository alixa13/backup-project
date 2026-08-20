# Build context must be ./supervised (parent of API) so we can include final.py and models.
# python:3.12, not 3.9: the shipped model needs scikit-learn 1.8.0, which requires
# Python >=3.11. See API/requirements.txt for the full reasoning.
FROM python:3.12

WORKDIR /app

# API dependencies (upgrade pip so PyPI is reachable)
COPY API/requirements.txt .
RUN pip install --upgrade pip setuptools wheel \
    && pip install --no-cache-dir -r requirements.txt

# API app and entrypoint
COPY API/run.py .
COPY API/app ./app

# final.py for IDSModel (7-class); must be on Python path so "from final import IDSModel" works
COPY final.py .

# Models: ids_best*.pkl (7-class) + legacy joblib from API/app/models
COPY models/ /app/models/
COPY API/app/models/ /app/models/

EXPOSE 8000
CMD ["gunicorn", "--bind", "0.0.0.0:8000", "--workers", "1", "--threads", "4", "--timeout", "120", "run:app"]
