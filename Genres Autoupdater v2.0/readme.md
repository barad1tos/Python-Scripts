# Music Genre Updater

![License](https://img.shields.io/badge/license-MIT-blue.svg)
![Python](https://img.shields.io/badge/python-3.9%2B-blue.svg)

Music Genre Updater is an advanced Python-based tool that automatically manages your Apple Music library. It ensures consistency of genres across artists, cleans metadata (removing remaster tags and promotional text), retrieves album years from external APIs, and provides detailed analytics on all operations.

## Table of Contents

- [Music Genre Updater](#music-genre-updater)
  - [Table of Contents](#table-of-contents)
  - [Description](#description)
  - [Key April 2025 Updates](#key-april-2025-updates)
  - [Features](#features)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
    - [Clone the Repository](#clone-the-repository)
    - [Set Up a Virtual Environment](#set-up-a-virtual-environment)
    - [Install Dependencies](#install-dependencies)
    - [Configuration](#configuration)
    - [Setting Up the Launch Agent with launchctl](#setting-up-the-launch-agent-with-launchctl)
  - [Usage](#usage)
    - [Running the Script Manually](#running-the-script-manually)
    - [Command-Line Arguments](#command-line-arguments)
    - [Examples](#examples)
  - [Technical Architecture](#technical-architecture)
    - [Core Components](#core-components)
    - [Function Directory](#function-directory)
      - [Main Functions](#main-functions)
      - [AppleScript Integration](#applescript-integration)
      - [Metadata Cleaning Configuration](#metadata-cleaning-configuration)
      - [Genre Management](#genre-management)
      - [Album Year Retrieval](#album-year-retrieval)
      - [Database Management](#database-management)
      - [Utility Functions](#utility-functions)
    - [Service Layer](#service-layer)
      - [AppleScriptClient](#applescriptclient)
      - [CacheService](#cacheservice)
      - [ExternalApiService](#externalapiservice)
      - [DependencyContainer](#dependencycontainer)
  - [Logging System](#logging-system)
    - [Logger Types](#logger-types)
    - [Log Format and Structure](#log-format-and-structure)
    - [Log Levels and Colors](#log-levels-and-colors)
    - [Understanding Log Output](#understanding-log-output)
    - [Log Rotation and Management](#log-rotation-and-management)
  - [Analytics Framework](#analytics-framework)
    - [Performance Tracking](#performance-tracking)
    - [Duration Classification](#duration-classification)
    - [Event Storage and Memory Management](#event-storage-and-memory-management)
    - [HTML Report Generation](#html-report-generation)
    - [Statistical Aggregation](#statistical-aggregation)
  - [Data Flow and Process Model](#data-flow-and-process-model)
    - [Incremental Update Flow](#incremental-update-flow)
    - [Targeted Artist Update Flow](#targeted-artist-update-flow)
    - [Album Year Update Flow](#album-year-update-flow)
    - [Database Verification Flow](#database-verification-flow)
    - [Dry Run Simulation Flow](#dry-run-simulation-flow)
  - [Configuration Details](#configuration-details)
    - [my-config.yaml](#my-configyaml)
      - [Paths and Environment](#paths-and-environment)
      - [Performance Settings](#performance-settings)
      - [Testing and Development](#testing-and-development)
      - [Logging Configuration](#logging-configuration)
      - [Database Verification](#database-verification)
      - [Year Retrieval Configuration](#year-retrieval-configuration)
      - [Metadata Cleaning](#metadata-cleaning)
      - [Exception Handling](#exception-handling)
      - [Analytics Configuration](#analytics-configuration)
  - [AppleScript Implementation Details](#applescript-implementation-details)
    - [fetch_tracks.applescript](#fetch_tracksapplescript)
      - [fetch_tracks Key Operations](#fetch_tracks-key-operations)
      - [Example Output](#example-output)
    - [update_property.applescript](#update_propertyapplescript)
      - [update_property Key Operations](#update_property-key-operations)
      - [Example Usage from Python](#example-usage-from-python)
  - [Advanced Features](#advanced-features)
    - [Sophisticated Genre Determination Algorithm](#sophisticated-genre-determination-algorithm)
    - [Intelligent Album Year Scoring System](#intelligent-album-year-scoring-system)
    - [Incremental Processing Mechanism](#incremental-processing-mechanism)
    - [Error Recovery and Retry Logic](#error-recovery-and-retry-logic)
  - [Reporting System](#reporting-system)
    - [CSV Reporting](#csv-reporting)
    - [Analytics Reporting](#analytics-reporting)
    - [Console Reporting](#console-reporting)
  - [Auxiliary Scripts](#auxiliary-scripts)
    - [full_sync.py](#full_syncpy)
      - [Full Sync Functions](#full-sync-functions)
      - [Full Sync Operation](#full-sync-operation)
    - [dry_run.py](#dry_runpy)
      - [Dry Run Functions](#dry-run-functions)
      - [Dry Run Operation](#dry-run-operation)
  - [Contributing](#contributing)
  - [License](#license)
  - [Contacts](#contacts)
  - [Troubleshooting](#troubleshooting)
  - [FAQ](#faq)

## Description

Music Genre Updater intelligently manages your Apple Music library through advanced automation. It ensures that each artist has a consistent genre across all tracks (based on their earliest releases), removes clutter from track and album names (like "2023 Remaster" or "Deluxe Edition"), and retrieves accurate album years from reputable music databases (MusicBrainz and Discogs).

The tool is designed to work asynchronously with large music libraries, using sophisticated caching mechanisms to minimize API calls and AppleScript interactions. It features a complete analytics framework that tracks performance and provides detailed reports.

## Key April 2025 Updates

- **Track Database CSV** now includes `old_year` and `new_year` columns for every track, enabling robust year synchronization and change tracking.
- **Album Year Update Logic**: The script now skips albums that already have a definitive year in the database, minimizing redundant API calls and updates. Only albums with missing or outdated years are processed.
- **Year Caching**: Album years are cached both in-memory and in a persistent CSV (`cache_albums.csv`). This ensures that once a year is determined, it is reused for all relevant tracks and future runs.
- **Change Reporting**: All changes (genre, year, name, etc.) are logged in a unified changes report CSV, with clear `change_type`, `old_*`, and `new_*` fields, and timestamped for auditability.
- **Analytics & HTML Reports**: The analytics system tracks all major operations and generates a detailed HTML report after each run, including grouped statistics, per-function timing, and error/success rates.
- **AppleScript Integration**: Improved error handling, argument passing, and diagnostics for all AppleScript operations. Bulk updates and property changes are now more robust.
- **Dry Run & Full Sync**: Both dry run and full sync scripts use the new CSV structure and reporting, allowing safe preview and one-time full database refreshes.
- **Async Architecture**: All major operations (track fetching, AppleScript, API calls, cache) are fully asynchronous for maximum performance and reliability.

## Features

- **Comprehensive Genre Management**: Determines and applies the dominant genre for each artist based on their earliest releases
- **Metadata Cleaning**: Removes marketing text, remaster tags, and other clutter from track and album names
- **Album Year Retrieval**: Fetches accurate original release years from MusicBrainz and Discogs with intelligent scoring
- **Incremental Processing**: Updates only tracks added since the last run to minimize resources
- **Database Verification**: Checks for and removes tracks that no longer exist in your Music library
- **Asynchronous Architecture**: Handles large music libraries efficiently through concurrent operations
- **Advanced Caching**: Multi-tiered caching system for minimizing redundant operations
- **Analytics Framework**: Tracks execution time, overhead, and call counts with HTML report generation
- **Dry Run Simulation**: Tests changes without applying them to your music library
- **Detailed Logging**: Comprehensive logs for monitoring and debugging
- **Configurable Exception Handling**: Allows specific artists or albums to be excluded from processing
- **Year Synchronization**: Tracks and albums are now synchronized with `old_year` and `new_year` fields, ensuring accurate year updates and preventing redundant processing.
- **Unified Change Reporting**: All changes (genre, year, name, etc.) are consolidated in a single CSV report, with clear change types and before/after values.
- **Album Year Caching**: Album years are cached in both memory and a persistent CSV, minimizing API calls and ensuring consistency across runs.
- **Skip Processed Albums**: The script automatically skips albums that already have a definitive year, reducing unnecessary updates and API usage.
- **Robust Analytics**: HTML analytics reports now include grouped statistics, per-function timing, and error/success rates for all major operations.
- **Improved AppleScript Handling**: Enhanced error handling, diagnostics, and argument passing for all AppleScript operations, including bulk property updates.
- **Dry Run & Full Sync Support**: Both dry run and full sync scripts are updated to use the new CSV structure and reporting system.

## Prerequisites

- **Operating System**: macOS (tested on Sonoma)
- **Python**: Version 3.9 or higher
- **Apple Music**: Installed and configured
- **Brew**: Recommended for managing packages

## Installation

### Clone the Repository

```bash
git clone https://github.com/yourusername/music-genre-updater.git
cd music-genre-updater
```

### Set Up a Virtual Environment

```bash
python3 -m venv venv
source venv/bin/activate
```

### Install Dependencies

```bash
pip install PyYAML aiohttp
```

### Configuration

Copy the example configuration file and customize it:

```bash
cp my-config.yaml.example my-config.yaml
```

Edit `my-config.yaml` to match your environment:

```bash
nano my-config.yaml
```

Ensure all paths (music_library_path, apple_scripts_dir, logs_base_dir) are correctly set for your system.

### Setting Up the Launch Agent with launchctl

1. Create the LaunchAgents directory:

```bash
mkdir -p ~/Library/LaunchAgents
```

2. Create a plist file:

```bash
nano ~/Library/LaunchAgents/com.user.MusicGenreUpdater.plist
```

3. Add the following content:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
  <dict>
    <key>Label</key>
    <string>com.user.MusicGenreUpdater</string>

    <key>ProgramArguments</key>
    <array>
      <string>/usr/bin/python3</string>
      <string>/path/to/your/music_genre_updater.py</string>
    </array>

    <key>StartInterval</key>
    <integer>3600</integer> <!-- Runs every hour -->

    <key>WorkingDirectory</key>
    <string>/path/to/your/project/directory</string>

    <key>StandardOutPath</key>
    <string>/path/to/your/logs/music_genre_updater_stdout.log</string>

    <key>StandardErrorPath</key>
    <string>/path/to/your/logs/music_genre_updater_stderr.log</string>

    <key>EnvironmentVariables</key>
    <dict>
      <key>PATH</key>
      <string>/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin</string>
    </dict>
  </dict>
</plist>
```

4. Load the launch agent:

```bash
launchctl load ~/Library/LaunchAgents/com.user.MusicGenreUpdater.plist
```

## Usage

### Running the Script Manually

```bash
python music_genre_updater.py
```

### Command-Line Arguments

The script supports several modes of operation:

- **Default mode** (no arguments): Run incremental update of genres and clean names
- **--force**: Override incremental interval checks and force processing
- **--dry-run**: Simulate changes without modifying the Music library
- **clean_artist**: Process only tracks by a specific artist
  - **--artist**: Specify target artist
  - **--force**: Force operation regardless of previous runs
- **update_years**: Update album years from external APIs
  - **--artist**: Optionally limit to specific artist
  - **--force**: Force update even for albums with existing years
- **verify_database**: Check for and remove tracks that no longer exist in Music.app
  - **--force**: Force verification even if recently performed

### Examples

Run normal incremental update:

```bash
python music_genre_updater.py
```

Force full update of all tracks:

```bash
python music_genre_updater.py --force
```

Clean track names for a specific artist:

```bash
python music_genre_updater.py clean_artist --artist "Metallica"
```

Update album years for all artists:

```bash
python music_genre_updater.py update_years
```

Update album years for a specific artist, overriding existing values:

```bash
python music_genre_updater.py update_years --artist "Spiritbox" --force
```

Verify database integrity, removing non-existent tracks:

```bash
python music_genre_updater.py verify_database --force
```

Run in dry-run mode to simulate changes:

```bash
python music_genre_updater.py --dry-run
```

## Technical Architecture

### Core Components

The architecture follows a modular design with several key components:

1. **Entry Point (`music_genre_updater.py`)**: Parses arguments, initializes the environment, and routes to the appropriate execution path.
2. **Dependency Container (`dependencies_service.py`)**: Manages service instantiation and dependency injection.
3. **Service Layer**: Specialized services that handle distinct responsibilities:
   - **AppleScriptClient**: Interface to Music.app via AppleScript
   - **CacheService**: Manages in-memory and persistent caching
   - **ExternalApiService**: Handles external API communication
4. **Utility Modules**:
   - **Logger**: Custom logging framework with color coding and rotation
   - **Analytics**: Performance tracking and reporting system
   - **Reports**: CSV and HTML report generation

### Function Directory

This section documents the key functions within the codebase, their purposes, and interactions.

#### Main Functions

- **`main()`** (music_genre_updater.py)

  - **Purpose**: Entry point to the application
  - **Operation**: Parses command-line arguments, sets up the environment, and delegates to `main_async()`
  - **Logging**: Logs script start with arguments and overall execution time
  - **Example Log**: `14:05:22 I Starting script with arguments: {'force': True, 'command': None}`

- **`main_async(args)`** (music_genre_updater.py)

  - **Purpose**: Asynchronous core logic coordinator
  - **Operation**: Based on arguments, routes to appropriate execution path (incremental update, clean_artist, update_years, verify_database)
  - **Logging**: Detailed progress logs and performance metrics
  - **Example Log**: `14:05:24 I Found 2437 tracks in Music.app`

- **`parse_arguments()`** (music_genre_updater.py)
  - **Purpose**: Command-line argument parsing
  - **Operation**: Defines and parses all command-line options using argparse
  - **Returns**: Namespace with parsed arguments

#### AppleScript Integration

- **`run_applescript_async(script_name, args)`** (music_genre_updater.py)

  - **Purpose**: Execute AppleScript files with arguments
  - **Operation**: Invokes the AppleScriptClient service to run named scripts
  - **Analytics**: Tracked with the "AppleScript Execution" event type
  - **Logging**: Logs script name, arguments, execution time, and result preview
  - **Example Log**: `14:05:25 I Running AppleScript: fetch_tracks.applescript with args: 'Metallica'`

- **`fetch_tracks_async(artist, force_refresh)`** (music_genre_updater.py)

  - **Purpose**: Retrieve track data from Music.app
  - **Operation**: Checks cache first, falls back to AppleScript if needed
  - **Analytics**: Tracked with the "Fetch Tracks Async" event type
  - **Logging**: Records cache hits/misses and fetch results
  - **Example Log**: `14:05:26 I Using cached data for Metallica, found 98 tracks`

- **`parse_tracks(raw_data)`** (music_genre_updater.py)
  - **Purpose**: Parse raw AppleScript output into track dictionaries
  - **Operation**: Splits the raw string by newlines and field delimiters
  - **Analytics**: Tracked with the "Parse Tracks" event type
  - **Logging**: Reports total parsed track count
  - **Example Log**: `14:05:26 I Successfully parsed 2437 tracks from Music.app`

#### Metadata Cleaning Configuration

- **`clean_names(artist, track_name, album_name)`** (music_genre_updater.py)

  - **Purpose**: Remove marketing tags and clutter from names
  - **Operation**: Uses configurable pattern matching to clean metadata
  - **Analytics**: Tracked with the "Clean Names" event type
  - **Logging**: Shows before/after comparisons
  - **Example Log**: `14:05:30 I Original track name: 'Master of Puppets (Remastered)' -> 'Master of Puppets'`

- **`remove_parentheses_with_keywords(name, keywords)`** (music_genre_updater.py)
  - **Purpose**: Advanced parenthetical content removal
  - **Operation**: Uses a stack-based algorithm to handle nested parentheses
  - **Analytics**: Not directly tracked, but indirectly through `clean_names`
  - **Example Operation**: `"Master of Puppets (2021 Remastered Version)"` → `"Master of Puppets"`

#### Genre Management

- **`update_genres_by_artist_async(tracks, last_run_time)`** (music_genre_updater.py)

  - **Purpose**: Core genre update functionality
  - **Operation**: Groups tracks by artist, determines dominant genre, updates all tracks
  - **Analytics**: Tracked with the "Update Genres by Artist" event type
  - **Logging**: Reports artist count, dominant genres determined, and updates applied
  - **Example Log**: `14:05:35 I Artist: Metallica, Dominant Genre: Thrash Metal (from 98 tracks)`

- **`group_tracks_by_artist(tracks)`** (music_genre_updater.py)

  - **Purpose**: Organize tracks by artist for efficient processing
  - **Operation**: Creates a dictionary mapping artists to their tracks
  - **Analytics**: Tracked with the "Group Tracks by Artist" event type
  - **Logging**: Not directly logged

- **`determine_dominant_genre_for_artist(artist_tracks)`** (music_genre_updater.py)

  - **Purpose**: Identify the representative genre for an artist
  - **Operation**: Uses earliest added track from earliest album as reference
  - **Analytics**: Tracked with the "Determine Dominant Genre" event type
  - **Logging**: Not directly logged but results appear in update_genres_by_artist_async logs
  - **Algorithm**:
    1. Finds the earliest track for each album
    2. Determines the earliest album by addition date
    3. Uses the genre of the earliest track in that album

- **`update_track_async(track_id, new_track_name, new_album_name, new_genre, new_year)`** (music_genre_updater.py)
  - **Purpose**: Apply changes to a single track in Music.app
  - **Operation**: Calls update_property.applescript with appropriate parameters
  - **Analytics**: Tracked with the "Update Track" event type
  - **Logging**: Reports success/failure for each property update
  - **Example Log**: `14:05:40 I Updated genre for 12345 to Thrash Metal`

#### Album Year Retrieval

- **`process_album_years(tracks, force)`** (music_genre_updater.py)

  - **Purpose**: Coordinator for album year update process
  - **Operation**: Initializes API service, calls update function, handles results
  - **Analytics**: Not directly tracked
  - **Logging**: Reports start/end of year update process and results
  - **Example Log**: `14:05:45 I Starting album year updates (force=False)`

- **`update_album_years_async(tracks, force)`** (music_genre_updater.py)

  - **Purpose**: Core album year update functionality
  - **Operation**: Groups tracks by album, queries APIs, updates tracks with years
  - **Analytics**: Tracked with the "Update Album Years" event type
  - **Logging**: Reports batch progress and API calls
  - **Example Log**: `14:05:50 I Processing batch 1/5`

- **`update_album_tracks_bulk_async(track_ids, year)`** (music_genre_updater.py)
  - **Purpose**: Apply year updates to multiple tracks efficiently
  - **Operation**: Processes tracks in batches, updates year property
  - **Analytics**: Tracked with the "Update Album Tracks Bulk" event type
  - **Logging**: Reports batch results and success/failure counts
  - **Example Log**: `14:05:55 I Year update results: 45 successful, 0 failed`

#### Database Management

- **`verify_and_clean_track_database(force)`** (music_genre_updater.py)

  - **Purpose**: Validate CSV database against Music.app
  - **Operation**: Checks if tracks still exist, removes invalid entries
  - **Analytics**: Tracked with the "Database Verification" event type
  - **Logging**: Reports verification progress and results
  - **Example Log**: `14:06:00 I Found 3 tracks that no longer exist in Music.app`

- **`sync_track_list_with_current(all_tracks, csv_path, cache_service, logger, error_logger, partial_sync)`** (reports.py)
  - **Purpose**: Merge current tracks with CSV database, including `old_year` and `new_year` fields for robust year tracking.
  - **Operation**: Updates existing entries, adds new ones, preserves album years, and ensures that already-processed albums are skipped in future runs.
  - **Logging**: Reports sync statistics, including how many tracks were added/updated and how many albums were skipped due to existing year data.
  - **Example Log**: `14:06:05 I Added/Updated 52 tracks in CSV. Skipped 10 albums with existing year.`

#### Utility Functions

- **`is_music_app_running()`** (music_genre_updater.py)

  - **Purpose**: Check if Apple Music is active
  - **Operation**: Uses AppleScript to query running processes
  - **Analytics**: Tracked with the "Check Music App Running" event type
  - **Logging**: Not directly logged in normal operation
  - **Error Log**: `14:06:10 E Music app is not running! Please start Music.app before running this script.`

- **`can_run_incremental(force_run)`** (music_genre_updater.py)

  - **Purpose**: Determine if incremental update is due
  - **Operation**: Checks last run time against configured interval
  - **Analytics**: Tracked with the "Can Run Incremental" event type
  - **Logging**: Reports last run time and next scheduled run
  - **Example Log**: `14:06:15 I Last run: 2023-03-15 13:45. Next run in 10 mins.`

- **`update_last_incremental_run()`** (music_genre_updater.py)
  - **Purpose**: Record current time as last run timestamp
  - **Operation**: Writes current datetime to configured file
  - **Analytics**: Tracked with the "Update Last Incremental Run" event type

### Service Layer

The service layer provides modular, reusable functionality that's injected into the main application.

#### AppleScriptClient

The `AppleScriptClient` class (services/applescript_client.py) provides the interface between Python and AppleScript:

- **Key Methods**:
  - **`run_script(script_name, arguments, timeout)`**: Executes a named AppleScript file
  - **`run_script_code(script_code, arguments, timeout)`**: Executes raw AppleScript code
- **Important Features**:
  - **Concurrency Control**: Uses a semaphore to limit parallel AppleScript executions
  - **Timeout Management**: Cancels scripts that run too long
  - **Robust Error Handling**: Captures and processes AppleScript errors
- **Logging**:
  - Script execution with arguments and timeout
  - Execution time and result size
  - Error conditions with specific error messages
- **Example Log**:

  ```md
  14:06:20 I ▷ fetch_tracks.applescript (args: Metallica) [t:600s]
  14:06:22 I ◁ fetch_tracks.applescript (24567B, 2.1s) 1234~|~Master of Puppets~|~Met...
  ```

#### CacheService

The `CacheService` class (services/cache_service.py) provides multi-tiered caching:

- **Caching Levels**:
  - **In-Memory Cache**: Fast key-value store with TTL support
  - **Album Year CSV Cache**: Persistent storage for album years
  - **API Cache JSON**: Persists generic API responses between runs
- **Key Methods**:
  - **`get_async(key_data, compute_func)`**: Fetch from cache or compute if missing
  - **`set_async(key_data, value, ttl)`**: Store value in cache with optional TTL
  - **`load_cache()` / `save_cache()`**: Persist generic API cache to JSON file
  - **`get_album_year_from_cache(artist, album)`**: Get album year from CSV
  - **`store_album_year_in_cache(artist, album, year)`**: Save album year to CSV
- **Important Features**:
  - **Key Hashing**: Converts complex keys to hashable strings
  - **TTL Support**: Automatic expiration of stale cache entries
  - **Memory Management**: Prevents unbounded growth for long-running instances
- **Logging**:
  - Cache hits/misses
  - Album year storage and retrieval
  - Cache invalidation events
- **Example Log**:

  ```md
  14:06:25 I Cache hit for Metallica
  14:06:30 I Stored year 1986 for 'Metallica - Master of Puppets' in cache
  ```

#### ExternalApiService

The `ExternalApiService` class (services/external_api_service.py) handles communication with music databases:

- **Supported APIs**:
  - **MusicBrainz**: Primary source for album metadata
  - **Discogs**: Secondary source with complementary data
- **Key Methods**:
  - **`get_album_year(artist, album)`**: Determine the original release year
  - **`get_artist_activity_period(artist)`**: Identify artist's active years
  - **`_score_original_release(release, artist, album)`**: Score potential release matches
- **Important Features**:
  - **Rate Limiting**: Respects API rate limits to prevent throttling
  - **Scoring Algorithm**: Sophisticated release scoring based on multiple factors
  - **Artist Context**: Uses artist career timeline to validate years
- **Logging**:
  - API requests and responses
  - Scoring details for potential matches
  - Selected year with justification
- **Example Log**:

  ```md
  14:06:35 I Searching for original release year: 'Metallica - Master of Puppets'
  14:06:40 I Artist activity period: 1981 - present
  14:06:45 I Found 5 release groups for Metallica - Master of Puppets
  14:06:50 I Release group info - Type: Album, First release date: 1986-03-03
  14:06:55 I Year scores: 1986:95, 2016:45, 2023:30
  14:07:00 I Selected best year by score: 1986 (score: 95)
  ```

#### DependencyContainer

The `DependencyContainer` class (services/dependencies_service.py) implements dependency injection:

- **Purpose**: Centralize service instantiation and configuration
- **Key Properties**:
  - **`ap_client`**: AppleScriptClient instance
  - **`cache_service`**: CacheService instance
  - **`external_api_service`**: ExternalApiService instance
  - **`analytics`**: Analytics instance
  - **`config`**: Configuration dictionary
- **Initialization**:
  - Loads configuration from YAML
  - Creates loggers
  - Instantiates all services with appropriate dependencies

## Logging System

The logging system provides detailed visibility into the operation of the tool.

### Logger Types

The application uses three specialized loggers:

1. **Console Logger (`console_logger`)**

   - **Purpose**: Real-time feedback during execution
   - **Format**: Compact time, colored level, message
   - **Default Level**: INFO
   - **Output**: Standard output (terminal)

2. **Error Logger (`error_logger`)**

   - **Purpose**: Detailed error tracking and diagnostics
   - **Format**: Timestamp, level, message, optional stack trace
   - **Default Level**: INFO (captures INFO and above)
   - **Output**: File-based (`main_log_file` in config)

3. **Analytics Logger (`analytics_logger`)**
   - **Purpose**: Performance tracking and statistics
   - **Format**: Timestamp, level, operation, duration, status
   - **Default Level**: INFO
   - **Output**: File-based (`analytics_log_file` in config)

### Log Format and Structure

The log format follows these patterns:

- **Console Format**: `HH:MM:SS LEVEL_ABBR MESSAGE`
- **File Format**: `YYYY-MM-DD HH:MM:SS LEVEL_ABBR MESSAGE`

Special log elements:

- **Run Headers/Footers**: Separators that mark the start and end of script runs
- **Section Dividers**: Optional separators between logical operations
- **Duration Indicators**: Emoji-based indicators of operation speed (fast, medium, slow)
- **Status Indicators**: Success (✅) or failure (❌) markers

### Log Levels and Colors

Log levels use color coding for quick visual identification:

- **DEBUG (D)**: Gray - Detailed diagnostic information
- **INFO (I)**: Green - General operational information
- **WARNING (W)**: Yellow - Potential issues that don't prevent execution
- **ERROR (E)**: Red - Errors that impeded specific operations
- **CRITICAL (C)**: Magenta+Bold - Severe errors that prevented execution

### Understanding Log Output

When the script runs, you'll see log entries that follow these patterns:

1. **Script Initialization**:

   ```md
   14:07:05 I Starting script with arguments: {'force': False, 'command': None}
   ```

2. **AppleScript Execution**:

   ```md
   14:07:10 I Running AppleScript: fetch_tracks.applescript with args:
   14:07:15 I AppleScript fetch_tracks.applescript executed successfully, got 24567 bytes
   ```

3. **Track Processing**:

   ```md
   14:07:20 I Successfully parsed 2437 tracks from Music.app
   14:07:25 I Artist: Metallica, Dominant Genre: Thrash Metal (from 98 tracks)
   ```

4. **Track Updates**:

   ```md
   14:07:30 I ✅ Updated genre for track 12345 to Thrash Metal
   14:07:32 I ✅ Cleaning track ID 12346 - 'Master of Puppets (Remastered)'
   14:07:33 I Original track name: 'Master of Puppets (Remastered)' -> 'Master of Puppets'
   ```

5. **Performance Analytics**:

   ```md
   14:07:35 I ✅ ⚡ update_track_async (API Call) took 0.245s
   14:07:40 I ✅ ⏱️ update_album_years_async (API Call) took 3.721s
   14:07:45 I ✅ 🐢 verify_and_clean_track_database (Database) took 15.326s
   ```

6. **Summary Statistics**:

   ```md
   14:07:50 I 📊 Analytics Summary: 327 calls, 98.5% success, avg 0.754s
   14:07:55 I 📊 Performance: ⚡ 75% | ⏱️ 18% | 🐢 7%
   ```

### Log Rotation and Management

Logs are managed to prevent unbounded growth:

- **Run Tracking**: Each execution creates a run header and footer
- **Run Limit**: Only the most recent N runs are kept (configurable)
- **Directory Structure**: Logs are organized in subdirectories by type

## Analytics Framework

The analytics framework provides comprehensive performance tracking.

### Performance Tracking

Analytics are collected using Python decorators:

- **Key Function**`@analytics.track("Event Type")`
- **What's Tracked**:
  - Function name and event type
  - Start and end timestamps
  - Execution duration
  - Success/failure status
  - Decorator overhead

### Duration Classification

Operations are classified by duration:

- **Fast (⚡)**: Under 2 seconds (configurable)
- **Medium (⏱️)**: 2-5 seconds (configurable)
- **Slow (🐢)**: Over 5 seconds (configurable)

These thresholds are configurable in the `analytics.duration_thresholds` section of the configuration.

### Event Storage and Memory Management

Analytics data is stored with memory safety in mind:

- **In-Memory Storage**: Events stored in a list with configurable limit
- **Memory Management**: Oldest 10% of events pruned when limit reached
- **Event Format**: Structured dictionaries with standard fields
- **Pruning Logic**: Events can be pruned by age for long-running applications

### HTML Report Generation

The framework generates comprehensive HTML reports:

- **Report Types**:
  - **Incremental**: Shows events from the current run
  - **Full**: Shows events across multiple runs
- **Report Sections**:
  - **Summary**: Overall statistics and success rates
  - **Short Calls**: Grouped successful short-duration calls
  - **Detailed Calls**: Individual listings for long or failed calls
  - **Function Summary**: Per-function call counts and success rates

### Statistical Aggregation

Analytics provides aggregated statistics:

- **Success Rate**: Percentage of successful function calls
- **Average Duration**: Mean execution time across functions
- **Duration Distribution**: Breakdown of fast/medium/slow operations
- **Total Time**: Cumulative execution time by function
- **Overhead Analysis**: Impact of instrumentation on performance

## Data Flow and Process Model

This section details the data flow for each major operation mode.

### Incremental Update Flow

The standard incremental update follows this process:

1. **Initialization**:

   - Parse command-line arguments
   - Load configuration
   - Check if incremental interval has passed (`can_run_incremental()`)
   - Initialize dependency container and services

2. **Data Retrieval**:

   - Fetch tracks from Music.app (`fetch_tracks_async()`)
   - Parse raw data into track dictionaries (`parse_tracks()`)

3. **Track Processing**:

   - For newly added tracks (since last run):
     - Clean track and album names (`clean_track()`)
     - Group tracks by artist (`group_tracks_by_artist()`)
     - Determine dominant genre for each artist (`determine_dominant_genre_for_artist()`)
     - Update track genres with dominant genre (`update_track_async()`)

4. **CSV Database Update**:

   - Save changes to CSV database (`save_to_csv()`)
   - Generate changes report for traceability (`save_changes_report()`)
   - Update the album years in the database if necessary (`process_album_years()`)

5. **Finalization**:
   - Update the last incremental run timestamp (`update_last_incremental_run()`)
   - Generate analytics report (`analytics.generate_reports()`)
   - Log summary statistics (`analytics.log_summary()`)

### Targeted Artist Update Flow

When running in `clean_artist` mode:

1. **Initialization**:

   - Parse command-line arguments, extracting target artist
   - Load configuration and initialize services
   - Verify Music.app is running (`is_music_app_running()`)

2. **Artist-Specific Retrieval**:

   - Fetch only tracks by the specified artist (`fetch_tracks_async(artist=artist)`)
   - Parse track data into dictionaries (`parse_tracks()`)

3. **Targeted Processing**:

   - For each track by the artist:
     - Clean track and album names (`clean_names()`)
     - Update track if changes are needed (`update_track_async()`)
     - Track changes for reporting (`changes_log.append()`)

4. **Optional Year Update**:

   - If enabled, process album years for the artist (`process_album_years()`)

5. **Reporting**:
   - Synchronize updated tracks with database (`sync_track_list_with_current()`)
   - Generate changes report for the artist (`save_changes_report()`)
   - Generate analytics summary

### Album Year Update Flow

When running in `update_years` mode:

1. **Initialization**:

   - Parse arguments, checking for artist filter and force flag
   - Initialize ExternalApiService
   - Verify existence of cache directories

2. **Data Preparation**:

   - Fetch required tracks (`fetch_tracks_async()`)
   - Group tracks by album (`albums = {}`)

3. **Album Processing Loop**:

   - For each album (processed in batches):
     - Check cache first (`get_album_year_from_cache()`)
     - If not in cache or force=True, query APIs:
       - Call MusicBrainz API (`_get_scored_releases_from_musicbrainz()`)
       - Call Discogs API (`_get_scored_releases_from_discogs()`)
       - Score potential release years (`_score_original_release()`)
       - Select best year based on scores
     - Store year in cache (`store_album_year_in_cache()`)

4. **Track Updates**:

   - For each album with a determined year:
     - Update all tracks with that year (`update_album_tracks_bulk_async()`)
     - Track changes for reporting

5. **Database Updates**:
   - Update CSV database with new years (`sync_track_list_with_current()`)
   - Generate changes report (`save_changes_report()`)
   - Update last incremental run timestamp

### Database Verification Flow

When running in `verify_database` mode:

1. **Initialization**:

   - Parse command-line arguments, checking for force flag
   - Load existing database from CSV (`load_track_list()`)
   - Check if verification is due (based on last verify date)

2. **Track Filtering**:

   - Apply test_artists filter if configured
   - Log number of tracks to verify

3. **Verification Process**:

   - Process tracks in batches to reduce load
   - For each track ID:
     - Verify existence in Music.app (`verify_track_exists()`)
     - Add to invalid list if not found

4. **Database Cleanup**:

   - Remove invalid tracks from database
   - Save updated database to CSV (`save_to_csv()`)
   - Record verification date to prevent unnecessary rechecks

5. **Reporting**:
   - Log verification statistics
   - Generate analytics report

### Dry Run Simulation Flow

When running in `--dry-run` mode:

1. **Initialization**:

   - Import dry_run module
   - Log dry run mode

2. **Simulation Functions**:

   - Simulate cleaning (`simulate_cleaning()`):
     - Fetch tracks directly from Music.app
     - Apply cleaning rules without actual updates
     - Record potential changes
   - Simulate genre updates (`simulate_genre_update()`):
     - Determine dominant genres for artists
     - Compare with current genres
     - Record potential changes

3. **Report Generation**:

   - Save cleaning simulation to CSV (`save_cleaning_csv()`)
   - Save genre simulation to CSV (`save_genre_csv()`)
   - Generate unified report (`save_unified_dry_run()`)

4. **Console Output**:
   - Display summary of potential changes
   - Provide guidance on reviewing results

## Configuration Details

### my-config.yaml

The configuration file provides extensive customization options. Here's a detailed breakdown of each section:

#### Paths and Environment

```yaml
# Core paths
music_library_path: /Users/username/Music/Music/Music Library.musiclibrary
apple_scripts_dir: /path/to/apple/scripts
logs_base_dir: /path/to/logs
```

- **music_library_path**: Full path to the iTunes/Music library file (essential)
- **apple_scripts_dir**: Directory containing the AppleScript files
- **logs_base_dir**: Base directory for all logs, reports, and cache files

#### Performance Settings

```yaml
# Performance settings
apple_script_concurrency: 2 # Number of concurrent AppleScript calls
cache_ttl_seconds: 1800 # Cache lifetime (30 min)
applescript_timeout_seconds: 900 # AppleScript execution timeout (15 min)
incremental_interval_minutes: 15 # Interval between incremental runs
batch_size: 20 # Number of tracks to process in parallel
max_retries: 3 # Retry attempts for failed operations
retry_delay_seconds: 1 # Delay between retries
```

- **apple_script_concurrency**: Limits concurrent AppleScript executions to prevent system overload
- **cache_ttl_seconds**: Time-to-live for cached data before refreshing
- **applescript_timeout_seconds**: Maximum wait time for AppleScript execution
- **incremental_interval_minutes**: Minimum time between automatic incremental runs
- **batch_size**: Controls parallel processing batch size for performance
- **max_retries**: Number of retry attempts for failed operations
- **retry_delay_seconds**: Wait time between retry attempts

#### Testing and Development

```yaml
# Optional: limit to specific artists for testing
test_artists: [] # Example: ["Metallica", "Spiritbox"]
```

- **test_artists**: When populated, limits operations to specified artists (for testing)

#### Logging Configuration

```yaml
# Logging configuration
logging:
  max_runs: 3 # Keep logs for N most recent runs
  main_log_file: main/main.log
  csv_output_file: csv/track_list.csv
  changes_report_file: csv/changes_report.csv
  analytics_log_file: analytics/analytics.log
  last_incremental_run_file: last_incremental_run.log
```

- **max_runs**: Number of script runs to retain in log files
- **main_log_file**: Path to main log file (relative to logs_base_dir)
- **csv_output_file**: Path to track database CSV
- **changes_report_file**: Path to changes report CSV
- **analytics_log_file**: Path to performance analytics log
- **last_incremental_run_file**: File tracking last incremental run timestamp

#### Database Verification

```yaml
# Database verification settings
verify_database:
  batch_size: 20
  pause_seconds: 0.2
  auto_verify_days: 7
```

- **batch_size**: Number of tracks to verify in each batch
- **pause_seconds**: Delay between batches to reduce system load
- **auto_verify_days**: Automatically verify database every X days

#### Year Retrieval Configuration

```yaml
# Year retrieval configuration
year_retrieval:
  enabled: true
  preferred_api: musicbrainz
  discogs_token: "your_token_here"
  batch_size: 10
  delay_between_batches: 60
  min_valid_year: 1900
  trust_threshold: 50
  scoring:
    default_base_score: 20
    exact_match_bonus: 20
    official_status_bonus: 15
    bootleg_penalty: -40
    # ...and more scoring parameters
```

- **enabled**: Master switch for year retrieval functionality
- **preferred_api**: Primary API to query (musicbrainz or discogs)
- **discogs_token**: Authentication token for Discogs API
- **batch_size**: Albums to process in each batch
- **delay_between_batches**: Wait time between batches (API rate limiting)
- **min_valid_year**: Earliest acceptable year (prevents incorrect data)
- **trust_threshold**: Confidence score threshold for accepting a year
- **scoring**: Parameter weights for the year scoring algorithm:
  - **default_base_score**: Starting score for each potential match
  - **exact_match_bonus**: Points for exact album name match
  - **official_status_bonus**: Points for official status (vs bootleg)
  - **bootleg_penalty**: Point reduction for bootleg releases

#### Metadata Cleaning

```yaml
# Track and album name cleaning
cleaning:
  remaster_keywords:
    - remaster
    - remastered
    - Re-recording
    - Redux
    - Expanded
    - Deluxe Edition
  album_suffixes_to_remove:
    - " - EP"
    - " - Single"
```

- **remaster_keywords**: Terms to identify and remove from parenthetical expressions
- **album_suffixes_to_remove**: Text to remove from the end of album names

#### Exception Handling

```yaml
# Exceptions for specific artists/albums
exceptions:
  track_cleaning:
    - artist: "Artist Name"
      album: "Album Name"
```

- **track_cleaning**: List of artist/album combinations to exempt from cleaning

#### Analytics Configuration

```yaml
# Analytics configuration
analytics:
  colors:
    short: "#90EE90" # Light green for fast operations
    medium: "#D3D3D3" # Light gray for medium operations
    long: "#FFB6C1" # Light pink for slow operations
  duration_thresholds:
    short_max: 2
    medium_max: 5
    long_max: 10
  max_events: 10000
```

- **colors**: HTML color codes for analytics report (by duration category)
- **duration_thresholds**: Time thresholds (seconds) for operation classification
- **max_events**: Maximum number of events to store in memory (prevents memory leaks)

## AppleScript Implementation Details

The tool uses AppleScript to interface with Music.app. Here's a detailed explanation of each script.

### fetch_tracks.applescript

This script retrieves track data from the Music library.

#### fetch_tracks Key Operations

1. **Parameter Handling**:
   - Accepts optional artist parameter to filter tracks
2. **Music.app Communication**:

   - Uses `tell application "Music"` block to interact with Music.app
   - Queries library playlist 1 (main library)
   - Fetches multiple properties in a batch query for efficiency

3. **Data Formatting**:

   - Extracts track properties into separate lists
   - Formats dates consistently (YYYY-MM-DD HH:MM:SS)
   - Escapes special characters to preserve delimiters

4. **Output Generation**:
   - Creates delimited strings with `~|~` as field separator
   - Each track is a separate line with newline delimiter
   - Returns a complete text block for parsing by Python

#### Example Output

```md
1234~|~Master of Puppets~|~Metallica~|~Master of Puppets~|~Metal~|~2019-05-20 15:30:45~|~downloaded~|~1986
1235~|~Battery~|~Metallica~|~Master of Puppets~|~Metal~|~2019-05-20 15:30:45~|~downloaded~|~1986
```

### update_property.applescript

This script modifies track properties in the Music library.

#### update_property Key Operations

1. **Parameter Handling**:

   - Requires three parameters: track ID, property name, property value
   - Validates input parameters

2. **Track Lookup**:

   - Finds the track by ID in the main library
   - Verifies track exists before attempting updates

3. **Property Update**:

   - Uses a conditional structure to update the appropriate property
   - Supports name, album, genre, and year properties
   - Performs type conversion for numeric fields (e.g., year)

4. **Status Reporting**:
   - Returns "Success" message with details on success
   - Returns formatted error message on failure

#### Example Usage from Python

```python
result = await run_applescript_async(
    "update_property.applescript",
    ["1234", "genre", "Thrash Metal"]
)
```

## Advanced Features

### Sophisticated Genre Determination Algorithm

The genre determination algorithm implements this specific logic:

1. **Per-Artist Grouping**:

   - Groups all tracks by artist name
   - Processes each artist independently

2. **Album-First Processing**:

   - For each artist, identifies distinct albums
   - Determines the earliest track for each album (by dateAdded)

3. **Earliest Album Identification**:

   - Identifies the earliest album by the artist in your library
   - This represents your first exposure to the artist

4. **Earliest Track Selection**:

   - From the earliest album, selects the earliest track
   - The genre of this track becomes the "dominant genre"

5. **Consistent Genre Application**:
   - Applies the dominant genre to all tracks by that artist
   - Logs changes and updates the database

The rationale behind this approach is that the genre of the first track you added by an artist represents your preferred categorization for that artist. This produces more consistent libraries than algorithmic genre detection.

### Intelligent Album Year Scoring System

The album year scoring system combines multiple factors to identify original release years:

1. **Multi-API Querying**:

   - Queries both MusicBrainz and Discogs APIs
   - Combines and normalizes results

2. **Artist Context**:

   - Determines artist's active period (start year, end year if applicable)
   - Uses this context to validate potential release years

3. **Sophisticated Scoring Algorithm**:

   - Base score for each potential release year
   - Bonuses for:
     - Exact title matches
     - Official status
     - Releases during artist's active period
     - Primary country releases (e.g., US for US artists)
   - Penalties for:
     - Reissues and remasters
     - Bootlegs and unofficial releases
     - Releases outside active period
     - Future dates (impossible for original releases)

4. **Year Clustering**:

   - Groups releases by year
   - Aggregates scores for each year
   - Identifies years with similar high scores
   - Selects earliest year among similarly scored candidates

5. **Confidence Threshold**:
   - Enforces minimum confidence threshold (trust_threshold)
   - Rejects years with insufficient evidence

### Incremental Processing Mechanism

The incremental processing system optimizes performance by:

1. **Last Run Tracking**:

   - Records timestamp of each successful run
   - Stores in persistent file (last_incremental_run_file)

2. **Interval Enforcement**:

   - Checks current time against last run plus interval
   - Skips execution if interval hasn't passed
   - Shows remaining time until next eligible run

3. **Change Detection**:

   - For genre updates, processes only tracks added since last run
   - Filters by dateAdded timestamp
   - Skips unchanged tracks

4. **Force Override**:

   - Provides `--force` flag to bypass interval check
   - Allows manual triggering of out-of-schedule runs

5. **Database Synchronization**:
   - Efficiently merges new and updated tracks with existing database
   - Preserves previously retrieved album years
   - Updates only changed fields

### Error Recovery and Retry Logic

The error recovery system provides resilience against transient issues:

1. **Exception Handling**:

   - Comprehensive try/except blocks throughout codebase
   - Detailed error logging with context and stack traces
   - Graceful degradation on failures

2. **Retry Mechanism**:

   - Configurable retry attempts (max_retries)
   - Progressive backoff with configurable delay (retry_delay_seconds)
   - Specific retry logic for network-dependent operations

3. **Partial Success Handling**:

   - Tracks successful and failed operations separately
   - Continues processing despite individual failures
   - Reports success/failure statistics

4. **Database Consistency**:

   - Synchronizes database even after partial failures
   - Records changes that were successfully applied
   - Allows subsequent runs to pick up where previous run failed

5. **API Fallback**:
   - Primary/secondary API architecture (MusicBrainz/Discogs)
   - Falls back to secondary API on primary failure
   - Combines results from both when available

## Reporting System

### CSV Reporting

The system generates multiple CSV reports:

1. **Track Database (track_list.csv)**:

   - Complete record of all processed tracks
   - Fields: id, name, artist, album, genre, dateAdded, trackStatus, old_year, new_year
   - Updated incrementally with new or changed tracks
   - Primary persistence mechanism

2. **Changes Report (changes_report.csv)**:

   - Record of modifications made to the Music library
   - Fields: change_type, artist, album, track_name, old_genre, new_genre, old_year, new_year, old_track_name, new_track_name, old_album_name, new_album_name, timestamp
   - Includes before/after values for changed fields
   - Timestamped entries for audit trail

### Analytics Reporting

The analytics system produces detailed performance reports:

1. **HTML Report**:

   - Interactive, color-coded performance visualization
   - Sections for summary, grouped calls, and detailed events
   - Success rate statistics and call counts
   - Color-coding based on performance categories

2. **Log Summary**:
   - Condensed summary of performance metrics
   - Total calls, success rate, average duration
   - Performance breakdown by category (fast/medium/slow)
   - Logged at the end of each run

### Console Reporting

The system provides real-time feedback via console:

1. **Progress Indicators**:

   - Shows current operation and progress
   - Reports batch completion (e.g., "Processed batch 3/5")
   - Displays metrics for important operations

2. **Change Notifications**:

   - Reports track updates with before/after values
   - Shows emoji indicators for success/failure
   - Visualizes performance with speed indicators (⚡/⏱️/🐢)

3. **Summary Statistics**:
   - Shows totals at the end of each operation
   - Reports elapsed time for major tasks
   - Displays final count of changes applied

## Auxiliary Scripts

### full_sync.py

This script performs a complete synchronization of the track database.

#### Full Sync Functions

1. **`run_full_sync()`**:
   - Entry point for the script
   - Initializes dependency container
   - Performs direct fetch from Music.app
   - Synchronizes all tracks to CSV database

#### Full Sync Operation

1. Direct communication with Music.app
2. Complete fetch of all tracks regardless of dateAdded
3. Full synchronization with CSV database
4. Preservation of album years for continuity

### dry_run.py

This script simulates changes without applying them to Music.app.

#### Dry Run Functions

1. **`simulate_cleaning()`**:

   - Simulates name cleaning operations
   - Records potential changes

2. **`simulate_genre_update()`**:

   - Determines dominant genres by artist
   - Records potential genre changes

3. **`save_unified_dry_run()`**:
   - Combines simulated changes into a report
   - Formats for easy review

#### Dry Run Operation

1. Direct fetch from Music.app (bypassing cache for accuracy)
2. Application of same logic as actual operations
3. Recording potential changes instead of applying them
4. Generation of comprehensive report for review

## Contributing

Contributions to the Music Genre Updater are welcome! Please follow these steps:

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/new-feature`
3. Make your changes
4. Commit with descriptive messages: `git commit -m "Add feature: description"`
5. Push to your fork: `git push origin feature/new-feature`
6. Submit a pull request

Please ensure your code follows the existing style and includes appropriate documentation and tests.

## License

This project is licensed under the MIT License.

## Contacts

- **Author**: Roman Borodavkin
- **Email**: [roman.borodavkin@gmail.com](mailto:roman.borodavkin@gmail.com)
- **GitHub**: [@barad1tos](https://github.com/barad1tos)
- **LinkedIn**: [Roman Borodavkin](https://www.linkedin.com/in/barad1tos/)

## Troubleshooting

If you encounter issues:

1. **Check logs**: Review log files in your configured logs directory
2. **Music.app not running**: Ensure Apple Music is open before running the script
3. **AppleScript errors**: Test AppleScript files directly:

   ```bash
   osascript /path/to/fetch_tracks.applescript
   ```

4. **Permission issues**: Ensure the script has access to your Music library
5. **Launch agent problems**: Check status and reload if necessary:

   ```bash
   launchctl list | grep MusicGenreUpdater
   launchctl unload ~/Library/LaunchAgents/com.user.MusicGenreUpdater.plist
   launchctl load ~/Library/LaunchAgents/com.user.MusicGenreUpdater.plist
   ```

## FAQ

**Q: How often should I run the updater?**

A: The script is designed to run incrementally, so it can be run as often as you add new music to your library. The `incremental_interval_minutes` setting prevents it from running too frequently.

**Q: Will the script affect my Apple Music cloud library?**

A: Yes, changes made by the script will sync to your iCloud Music Library. Make sure you're comfortable with the changes before running in non-dry-run mode.

**Q: How do I exclude specific artists or albums from processing?**

A: Add them to the `exceptions.track_cleaning` section in your `my-config.yaml` file:

```yaml
exceptions:
  track_cleaning:
    - artist: "Artist Name"
      album: "Album Name"
```

**Q: How do I backup my music library before running the updater?**

A: Apple Music doesn't provide a simple way to backup and restore metadata changes. The dry run mode helps preview changes before applying them.

**Q: How accurate is the album year retrieval?**

A: The system uses a sophisticated scoring algorithm across multiple music databases, weighing factors like release status, country, and artist timeline to identify the most likely original release year. For most mainstream albums, it's very accurate.

**Q: Can I run multiple instances of the script simultaneously?**

A: No, this can cause race conditions or conflicting updates. The script uses file-based locks to prevent multiple instances from running simultaneously.

**Q: How can I reset everything and start fresh?**

A: Delete the CSV files in your logs directory, particularly `track_list.csv` and `last_incremental_run.log`. This will cause the script to reprocess all tracks.

**Q: Does the script modify my music files?**

A: No, it only updates metadata in the Apple Music database. The actual music files remain untouched.

**Q: How does the script avoid reprocessing albums that already have a year?**

A: The script checks the `new_year` field for each album in the CSV database. If a definitive year is already present, the album is skipped during year update operations. This prevents redundant API calls and unnecessary updates.

**Q: How are album years cached and reused?**

A: Album years are cached both in-memory and in a persistent CSV (`cache_albums.csv`). When a year is determined for an album, it is stored in the cache and reused for all relevant tracks and future runs, unless a force update is requested.

**Q: What is the difference between `old_year` and `new_year` in the CSV?**

A: `old_year` represents the year previously stored in the library or fetched from Music.app, while `new_year` is the year determined by the script (from APIs or cache). This allows for clear tracking of changes and prevents accidental overwrites.
