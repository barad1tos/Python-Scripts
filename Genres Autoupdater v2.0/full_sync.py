# full_sync.py
import asyncio
from scripts.reports import sync_track_list_with_current
from music_genre_updater import fetch_tracks_async, console_logger, error_logger, CONFIG

async def run_full_sync():
    console_logger.info("Running one-time full sync from external script.")
    all_tracks = await fetch_tracks_async()
    if not all_tracks:
        console_logger.warning("No tracks fetched. Full sync aborted.")
        return
    sync_track_list_with_current(
        all_tracks,
        CONFIG["csv_output_file"],
        console_logger,
        error_logger,
        partial_sync=False
    )
    console_logger.info("track_list.csv updated successfully.")

if __name__ == "__main__":
    asyncio.run(run_full_sync())