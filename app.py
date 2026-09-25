"""WSGI entrypoint for Gunicorn.

Usage on Render:
  gunicorn app:app --bind 0.0.0.0:$PORT --workers 1 --worker-class gthread --threads 8 --timeout 120
"""

from live_scanner_server import app  # Flask app