"""
app/model_manager.py

Trusted IP management for supervised API.
UNSW42 model is loaded directly by app/logs/unsw42.py.
"""
import os
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("model_manager")

TRUSTED_IPS = set()
# In Docker, mount is at /app/models/trusted_ips.txt; set TRUSTED_IPS_PATH env to match
TRUSTED_IPS_PATH = os.getenv("TRUSTED_IPS_PATH", os.path.join(os.path.dirname(__file__), "models", "trusted_ips.txt"))


def load_trusted_ips():
    global TRUSTED_IPS
    try:
        if os.path.exists(TRUSTED_IPS_PATH):
            with open(TRUSTED_IPS_PATH, 'r') as f:
                TRUSTED_IPS = set(line.strip() for line in f if line.strip())
            logger.info(f"Loaded {len(TRUSTED_IPS)} trusted IPs")
    except Exception as e:
        logger.error(f"Error loading trusted IPs: {e}")


def save_trusted_ips():
    try:
        d = os.path.dirname(TRUSTED_IPS_PATH)
        if d:
            os.makedirs(d, exist_ok=True)
        with open(TRUSTED_IPS_PATH, 'w') as f:
            for ip in sorted(TRUSTED_IPS):
                f.write(f"{ip}\n")
    except Exception as e:
        logger.error(f"Error saving trusted IPs: {e}")


def add_trusted_ip(ip):
    TRUSTED_IPS.add(ip)
    save_trusted_ips()
    return True


def remove_trusted_ip(ip):
    if ip in TRUSTED_IPS:
        TRUSTED_IPS.remove(ip)
        save_trusted_ips()
        return True
    return False


def get_trusted_ips():
    return sorted(list(TRUSTED_IPS))


load_trusted_ips()
