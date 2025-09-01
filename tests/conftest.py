import os
import sys

# Ensure project root is on sys.path so tests can import binance_usdm_loader.py as a module
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
