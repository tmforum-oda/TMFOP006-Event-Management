"""
WSGI entrypoint for gunicorn: `gunicorn trap_receiver.wsgi:app`.

app.py's main() is still the entrypoint for local/dev runs (`python3 -m
trap_receiver.app`, used by docker-compose's quick-start path) since it also
owns the startup log banner. This module just does the same config load +
create_app() call, exposing the result as a module-level `app` gunicorn can
import -- gunicorn needs an object, not a function that calls app.run().
"""
import os

from .app import load_config, create_app

config_path = os.environ.get("CONFIG_PATH", "/app/config/config.yaml")
app = create_app(load_config(config_path))
