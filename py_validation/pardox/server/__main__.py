"""
pardox_server/__main__.py
Entry point: python -m pardox_server  (or via the 'pardox server' wrapper script)

Usage:
  python -m pardox_server [--lib /path/to/libpardox.so]
                          [--port 5235]
                          [--flask-port 5000]
                          [--config-dir ~/.pardox_server]
                          [--no-flask]
                          [--start]          # auto-start PG server on launch
"""
import argparse
import os
import sys

# ── Locate the library before importing engine (which loads it) ────────────
def _parse_args():
    p = argparse.ArgumentParser(prog='pardox server')
    p.add_argument('--lib',         default=None,
                   help='Absolute path to libpardox.so')
    p.add_argument('--port',        type=int, default=None,
                   help='PostgreSQL wire protocol port (default: 5235 or from config)')
    p.add_argument('--flask-port',  type=int, default=5001,
                   help='Flask web UI port (default: 5001)')
    p.add_argument('--config-dir',  default=os.path.join(os.path.expanduser('~'), '.pardox_server'),
                   help='Directory for server.json and databases.json')
    p.add_argument('--no-flask',    action='store_true',
                   help='Skip Flask web UI (run PG server only)')
    p.add_argument('--start',       action='store_true',
                   help='Auto-start the PostgreSQL server on launch')
    return p.parse_args()


def main():
    args = _parse_args()

    if args.lib:
        os.environ['PARDOX_LIB_PATH'] = args.lib

    # ── Deferred imports (lib path must be set first) ──────────────────────
    from pardox_server.engine    import PardoxEngine
    from pardox_server.registry  import Registry
    from pardox_server.pg_server import PardoXPostgresServer
    from pardox_server.flask_app import app as flask_app, init as flask_init

    # ── Config dir ─────────────────────────────────────────────────────────
    config_dir = args.config_dir
    os.makedirs(config_dir, exist_ok=True)
    config_path = os.path.join(config_dir, 'server.json')

    # ── Load / create server config ────────────────────────────────────────
    import json
    if os.path.exists(config_path):
        with open(config_path) as f:
            cfg = json.load(f)
    else:
        cfg = {'port': 5235, 'username': 'pardox_user', 'password': 'pardoX_secret'}
        with open(config_path, 'w') as f:
            json.dump(cfg, f, indent=2)

    if args.port:
        cfg['port'] = args.port

    # ── Initialise components ──────────────────────────────────────────────
    engine   = PardoxEngine(lib_path=args.lib)
    registry = Registry(config_dir)
    server   = PardoXPostgresServer(
        engine, registry,
        port     = cfg['port'],
        username = cfg['username'],
        password = cfg['password'],
    )

    # ── Wire Flask ─────────────────────────────────────────────────────────
    flask_init(server, registry, config_path)

    if args.start:
        server.start()
        print(f"[pardox_server] PostgreSQL server started on port {cfg['port']}")

    if args.no_flask:
        # Block forever (PG server runs in a daemon thread)
        print("[pardox_server] Running without Flask UI. Press Ctrl+C to stop.")
        try:
            import time
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            server.stop()
        return

    # ── Start Flask ────────────────────────────────────────────────────────
    print("=" * 60)
    print("  pardoX Server")
    print(f"  PostgreSQL port : {cfg['port']}")
    print(f"  Web UI          : http://localhost:{args.flask_port}")
    print(f"  Config dir      : {config_dir}")
    print("=" * 60)
    flask_app.run(host='0.0.0.0', port=args.flask_port, debug=False, use_reloader=False)


if __name__ == '__main__':
    main()
