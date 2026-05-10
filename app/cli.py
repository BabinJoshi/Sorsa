from __future__ import annotations

import argparse
import asyncio
import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

from app.config import Settings
from app.db.connection import build_pool
from app.pipeline.orchestrator import IngestionOrchestrator

_DATETIME_FORMATS = (
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%d",
)


def _parse_datetime_arg(value: str) -> datetime:
    """Parse a user-supplied datetime string and return a UTC-aware datetime."""
    for fmt in _DATETIME_FORMATS:
        try:
            return datetime.strptime(value, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    raise argparse.ArgumentTypeError(
        f"Cannot parse datetime {value!r}. "
        "Accepted formats: YYYY-MM-DD, YYYY-MM-DD HH:MM, YYYY-MM-DD HH:MM:SS"
    )

LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
LOG_DATE_FMT = "%Y-%m-%d %H:%M:%S"


def _configure_logging() -> Path:
    """Set up console + rotating daily file logging.

    Log files are written to:
        logs/YYYY-MM-DD/run_HHMMSS_<8-char-id>.log

    Returns the path of the log file created for this run.
    """
    now = datetime.now(timezone.utc)
    run_tag = f"{now.strftime('%H%M%S')}_{uuid.uuid4().hex[:8]}"
    log_dir = Path("logs") / now.strftime("%Y-%m-%d")
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"run_{run_tag}.log"

    formatter = logging.Formatter(LOG_FORMAT, datefmt=LOG_DATE_FMT)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(formatter)

    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.addHandler(console_handler)
    root.addHandler(file_handler)

    return log_file


async def _run(
    project_keyword: str,
    hours: int,
    since: datetime | None,
    until: datetime | None,
) -> None:
    log_file = _configure_logging()
    load_dotenv()
    logger = logging.getLogger(__name__)
    logger.info("Log file: %s", log_file)

    settings = Settings()
    pool = await build_pool(settings)
    orchestrator = IngestionOrchestrator(settings, pool)
    pipeline_start = datetime.now(timezone.utc)
    try:
        run_id = await orchestrator.run_project_ingestion(
            project_keyword,
            hours=hours,
            since=since,
            until=until,
        )
        elapsed = datetime.now(timezone.utc) - pipeline_start
        total_minutes = elapsed.total_seconds() / 60
        logger.info(
            "Ingestion completed — run_id=%s | elapsed=%.1f min (%dm %ds)",
            run_id,
            total_minutes,
            int(total_minutes),
            int(elapsed.total_seconds() % 60),
        )
        print(f"Ingestion completed. run_id={run_id}  elapsed={total_minutes:.1f} min")
    finally:
        await orchestrator.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Sorsa ingestion runner")
    parser.add_argument(
        "--project-keyword",
        required=True,
        help=(
            "Comma-separated search terms for this project. "
            "Multiple terms are passed directly to the Sorsa query. "
            'Example: --project-keyword "quipnetwork,Quip Network,quip_network,Quip"'
        ),
    )

    window_group = parser.add_mutually_exclusive_group()
    window_group.add_argument(
        "--hours",
        type=int,
        default=72,
        help=(
            "How many hours back to search from now (default: 72). "
            "Ignored when --since is provided."
        ),
    )
    window_group.add_argument(
        "--since",
        type=_parse_datetime_arg,
        default=None,
        metavar="DATETIME",
        help=(
            "Start of the search window as a UTC datetime "
            "(e.g. '2026-05-08 23:41:00'). Mutually exclusive with --hours."
        ),
    )
    parser.add_argument(
        "--until",
        type=_parse_datetime_arg,
        default=None,
        metavar="DATETIME",
        help=(
            "End of the search window as a UTC datetime "
            "(e.g. '2026-05-09 04:11:00'). Defaults to now when --since is used."
        ),
    )

    args = parser.parse_args()
    asyncio.run(
        _run(
            args.project_keyword,
            hours=args.hours,
            since=args.since,
            until=args.until,
        )
    )


if __name__ == "__main__":
    main()

