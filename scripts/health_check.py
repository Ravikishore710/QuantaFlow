import argparse
import json
import sys
from pathlib import Path

# Ensure project root is on sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.common.logging import configure_logging  # noqa: E402
from src.monitoring.health import check_system_health, run_health_server  # noqa: E402


def main() -> None:
    configure_logging()

    parser = argparse.ArgumentParser(
        description="QuantaFlow system health check probe and HTTP server"
    )
    parser.add_argument(
        "--mode",
        choices=["cli", "server"],
        default="cli",
        help="Run health probe as CLI check or persistent HTTP server",
    )
    parser.add_argument(
        "--serve",
        action="store_true",
        default=False,
        help="Alias for --mode server",
    )
    parser.add_argument(
        "--host",
        type=str,
        default="0.0.0.0",
        help="Host interface to bind HTTP probe server",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8081,
        help="Port to bind HTTP probe server",
    )
    parser.add_argument(
        "--engine-mode",
        choices=["auto", "postgres", "duckdb"],
        default="auto",
        help="Database engine mode to verify during health check",
    )

    args = parser.parse_args()

    if args.serve or args.mode == "server":
        print(f"Starting QuantaFlow Health Probe Server on {args.host}:{args.port}...")
        run_health_server(host=args.host, port=args.port)
        return

    # CLI mode
    result = check_system_health(engine_mode=args.engine_mode)
    print(json.dumps(result, indent=2))

    if result.status_code if hasattr(result, "status_code") else result.get("status") == "healthy":
        sys.exit(0)
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()
