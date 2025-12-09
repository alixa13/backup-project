-- PostgreSQL initialization script for LSTM Autoencoder
-- Optimized for high-throughput write operations

-- Performance optimizations
ALTER SYSTEM SET shared_buffers = '256MB';
ALTER SYSTEM SET effective_cache_size = '1GB';
ALTER SYSTEM SET work_mem = '16MB';
ALTER SYSTEM SET maintenance_work_mem = '128MB';
ALTER SYSTEM SET max_connections = 200;
ALTER SYSTEM SET wal_buffers = '16MB';
ALTER SYSTEM SET max_wal_size = '2GB';
ALTER SYSTEM SET checkpoint_completion_target = 0.9;
ALTER SYSTEM SET random_page_cost = 1.1;
ALTER SYSTEM SET effective_io_concurrency = 200;

-- For async commit (faster writes, slight risk of data loss on crash)
-- ALTER SYSTEM SET synchronous_commit = 'off';  -- Uncomment for maximum speed (10×faster)

-- Connection pooling settings
ALTER SYSTEM SET max_prepared_transactions = 100;

-- Create database if it doesn't exist (handled by Docker env vars)
-- Database: lstm_db
-- User: lstm_user

-- Grant all privileges
GRANT ALL PRIVILEGES ON DATABASE lstm_db TO lstm_user;

-- Enable extensions if needed
-- CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

