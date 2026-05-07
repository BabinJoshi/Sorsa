from __future__ import annotations

import argparse
import asyncio

from dotenv import load_dotenv

from app.config import Settings
from app.pipeline.orchestrator import IngestionOrchestrator


async def _run(project_keyword: str) -> None:
    # Explicit dotenv loading as requested.
    load_dotenv()
    settings = Settings()
    orchestrator = IngestionOrchestrator(settings)
    try:
        run_id = await orchestrator.run_project_ingestion(project_keyword)
        print(f"Ingestion completed. run_id={run_id}")
    finally:
        await orchestrator.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Sorsa ingestion runner")
    parser.add_argument(
        "--project-keyword",
        required=True,
        help="Project keyword used for Sorsa search query",
    )
    args = parser.parse_args()
    asyncio.run(_run(args.project_keyword))


if __name__ == "__main__":
    main()

