#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Full Sync Module

This module runs a one-time full sync of the track list from the external API.
It fetches all tracks, updates the CSV file, and logs the changes.
"""

import asyncio

from music_genre_updater import CONFIG, console_logger, error_logger, fetch_tracks_async
from utils.reports import sync_track_list_with_current

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