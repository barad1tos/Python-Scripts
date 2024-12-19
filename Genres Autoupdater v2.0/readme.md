
![image](https://github.com/user-attachments/assets/ec7fc8b7-5825-4eb5-81ad-0dc5d9fb3755)

# Music Genre Updater

![License](https://img.shields.io/badge/license-MIT-blue.svg)
![Python](https://img.shields.io/badge/python-3.8%2B-blue.svg)
![GitHub Issues](https://img.shields.io/github/issues/yourusername/music-genre-updater)
![GitHub Forks](https://img.shields.io/github/forks/yourusername/music-genre-updater)
![GitHub Stars](https://img.shields.io/github/stars/yourusername/music-genre-updater)

Automated Music Genre Updater for Apple Music. This project leverages Python and AppleScript to analyze your music library, determine dominant genres for each artist, and update track genres accordingly.

## Table of Contents

- [Music Genre Updater](#music-genre-updater)
	- [Table of Contents](#table-of-contents)
	- [Description](#description)
	- [Features](#features)
	- [Prerequisites](#prerequisites)
	- [Installation](#installation)
		- [Clone the Repository](#clone-the-repository)
	- [Install Dependencies](#install-dependencies)
	- [Configuration](#configuration)
	- [Setting Up the Launch Agent with launchctl](#setting-up-the-launch-agent-with-launchctl)
- [Usage](#usage)
	- [Running the Script Manually](#running-the-script-manually)
	- [Command-Line Arguments](#command-line-arguments)
	- [Examples](#examples)
- [Configuration Details](#configuration-details)
	- [config.yaml](#configyaml)
- [Logging](#logging)
	- [Log Configuration:](#log-configuration)
	- [Log Files:](#log-files)
- [Auxiliary Scripts](#auxiliary-scripts)
	- [AppleScript Scripts](#applescript-scripts)
- [Contributing](#contributing)
- [License](#license)
- [Contact](#contact)
- [Troubleshooting](#troubleshooting)
- [FAQ](#faq)

## Description

**Music Genre Updater** is a Python-based tool designed to automatically update the genres of your music tracks in Apple Music. By analyzing your music library, it identifies the dominant genre for each artist and updates the genres of all their tracks accordingly. This ensures that your music library remains organized and accurately reflects your listening preferences.

## Features

- **Automatic Genre Updating:** Determines and updates the dominant genre for each artist based on track analysis.
- **Asynchronous Processing:** Utilizes asynchronous operations to handle large music libraries efficiently.
- **Detailed Logging:** Provides comprehensive logs for monitoring and debugging purposes.
- **YAML Configuration:** Easily configurable through a `config.yaml` file.
- **Scheduled Execution:** Uses `launchctl` to schedule regular updates automatically.
- **CSV Reporting:** Generates CSV reports of track details and changes made.
- **Exception Handling:** Supports exceptions to prevent certain artists or albums from being modified.
- **Robust Error Handling:** Retries failed updates with configurable parameters.

## Prerequisites

Before installing the Music Genre Updater, ensure you have the following:

- **Operating System:** macOS
- **Python:** Version 3.8 or higher
- **Apple Music:** Installed and configured
- **Homebrew:** Recommended for managing packages (optional but recommended)

## Installation

### Clone the Repository

Begin by cloning the repository to your local machine:

```bash
git clone https://github.com/yourusername/music-genre-updater.git
cd music-genre-updater
```
Set Up a Virtual Environment

It’s recommended to use a Python virtual environment to manage dependencies:

```bash
python3 -m venv venv
source venv/bin/activate
```

## Install Dependencies

Install the required Python packages using pip:

```bash
pip install PyYaml.
```

All other dependencies are the built-in Python libraries, starting from the 3.3 version.

## Configuration

Copy the example configuration file and customize it to fit your environment:

```bash
cp config.yaml.example config.yaml
```

Open config.yaml in your preferred text editor and update the paths and settings as needed. Detailed explanations of each configuration parameter are provided in the Configuration Details section below.

## Setting Up the Launch Agent with launchctl

To automate the execution of the Music Genre Updater, set up a launchctl agent:
	1.	Create the LaunchAgents Directory (if it doesn’t exist):

```bash
mkdir -p ~/Library/LaunchAgents
```

	2.	Create the plist File:
Create a file named com.barad1tos.MusicGenreUpdater.plist in the ~/Library/LaunchAgents/ directory:

```bash
nano ~/Library/LaunchAgents/com.barad1tos.MusicGenreUpdater.plist
```

	3.	Add the Following Content to the plist File:
```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
  <dict>
    <key>Label</key>
    <string>com.barad1tos.MusicGenreUpdater</string>
    
    <key>ProgramArguments</key>
    <array>
      <string>/usr/bin/python3</string>
      <string>/path/to/your/music_genre_updater.py</string>
    </array>
    
    <key>StartInterval</key>
    <integer>1800</integer> <!-- Runs every 30 minutes -->
    <key>KeepAlive</key>
    <false/>

    <key>WorkingDirectory</key>
    <string>/path/to/your/project/directory</string>
    
    <key>EnvironmentVariables</key>
    <dict>
      <key>PATH</key>
      <string>/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin</string>
    </dict>
    
    <key>StandardOutPath</key>
    <string>/path/to/your/logs/music_genre_updater_stdout.log</string>
    <key>StandardErrorPath</key>
    <string>/path/to/your/logs/music_genre_updater_stderr.log</string>
  </dict>
</plist>
```

Important:
	•	Replace /path/to/your/music_genre_updater.py with the actual path to your music_genre_updater.py script.
	•	Replace /path/to/your/project/directory with the path to your project’s root directory.
	•	Update the paths for StandardOutPath and StandardErrorPath to desired log file locations.

	4.	Load the Launch Agent:
Load the newly created agent using launchctl:

```bash
launchctl load ~/Library/LaunchAgents/com.barad1tos.MusicGenreUpdater.plist
```

	5.	Verify the Launch Agent is Loaded:
Check if the agent is running:

```bash
launchctl list | grep com.barad1tos.MusicGenreUpdater
```

If loaded successfully, you should see an entry corresponding to com.barad1tos.MusicGenreUpdater.

	6.	Unload the Launch Agent (Optional):
If you need to unload the agent in the future:

```bash
launchctl unload ~/Library/LaunchAgents/com.barad1tos.MusicGenreUpdater.plist
```

# Usage

You can run the Music Genre Updater manually or rely on the scheduled execution via launchctl.

## Running the Script Manually

Activate your virtual environment and execute the script:

```bash
source venv/bin/activate
python music_genre_updater.py
```

## Command-Line Arguments

The script supports several command-line arguments to customize its behavior:
	•	--force: Force the execution, bypassing incremental checks.
	•	clean_artist --artist "Artist Name": Clean track and album names for a specified artist.

## Examples

Force Run the Genre Update:

```bash
python music_genre_updater.py --force
```

Clean Track and Album Names for a Specific Artist:

```bash
python music_genre_updater.py clean_artist --artist "Rabbit Junk"
```

# Configuration Details

## config.yaml

The config.yaml file contains all the configuration settings for the Music Genre Updater. Below is a detailed explanation of each parameter:

```bash
# Path to the Apple Music library file
music_library_path: "/Users/yourusername/Music/Music/Music Library.musiclibrary"

# Directory containing AppleScript scripts
apple_scripts_dir: "/Users/yourusername/Path/To/AppleScripts"

# Path to the main log file
log_file: "/Users/yourusername/Path/To/Logs/music_genre_updater.log"

# Path to the CSV file containing the list of tracks
csv_output_file: "/Users/yourusername/Path/To/Outputs/track_list.csv"

# Path to the CSV file for changes report
changes_report_file: "/Users/yourusername/Path/To/Outputs/changes_report.csv"

# File to store the timestamp of the last incremental run
last_incremental_run_file: "/Users/yourusername/Path/To/Logs/last_incremental_run.log"

# Directory for backups
backup_dir: "/Users/yourusername/Path/To/Backups"

# Interval for incremental updates in minutes
incremental_interval_minutes: 60

# Maximum number of retries for updating genres
max_retries: 3

# Delay between retries in seconds
retry_delay_seconds: 2

# List of test artists for testing purposes (leave empty to process all)
test_artists: []

# Cleaning settings
cleaning:
  remaster_keywords:
    - remaster
    - remastered
  album_suffixes_to_remove:
    - ' - Single'
    - ' — EP'

# Exceptions for track cleaning
exceptions:
  track_cleaning:
    - artist: "Rabbit Junk"
      album: "Xenospheres"
```

Parameter Descriptions:
	•	music_library_path: Absolute path to your Apple Music library file.
	•	apple_scripts_dir: Directory where AppleScript files (fetch_tracks.applescript, update_property.applescript) are located.
	•	log_file: File path where logs will be stored.
	•	csv_output_file: CSV file path to save the list of tracks.
	•	changes_report_file: CSV file path to save reports of changes made.
	•	last_incremental_run_file: File to record the timestamp of the last incremental update run.
	•	backup_dir: Directory where backups will be stored.
	•	incremental_interval_minutes: Time interval in minutes between incremental update runs.
	•	max_retries: Maximum number of retry attempts for updating a genre.
	•	retry_delay_seconds: Delay in seconds between retry attempts.
	•	test_artists: List of specific artists to process for testing; leave empty to process all artists.
	•	cleaning.remaster_keywords: Keywords to identify and remove remaster information from track and album names.
	•	cleaning.album_suffixes_to_remove: Suffixes to remove from album names.
	•	exceptions.track_cleaning: List of artist and album combinations to exclude from cleaning.

# Logging

The project utilizes two loggers for comprehensive logging:
	1.	Console Logger (console_logger):
	•	Logs messages with a severity level of INFO and above to the console.
	•	Provides real-time feedback during script execution.
	2.	Error Logger (error_logger):
	•	Logs messages with a severity level of ERROR to a specified log file.
	•	Helps in diagnosing issues by providing detailed error information.

## Log Configuration:

Logging is configured in the logger.py module. The ColoredFormatter class adds color to log messages based on their severity:
	•	Errors: Displayed in red.
	•	Info Messages: Displayed in the default console color.

## Log Files:
	•	Standard Output Log: Defined by StandardOutPath in the plist file (e.g., music_genre_updater_stdout.log).
	•	Standard Error Log: Defined by StandardErrorPath in the plist file (e.g., music_genre_updater_stderr.log).
	•	Main Log File: Defined in config.yaml (log_file).

# Auxiliary Scripts

The project includes AppleScript scripts to interact with Apple Music. These scripts are essential for fetching track information and updating track properties.

## AppleScript Scripts
	1.	fetch_tracks.applescript:
	•	Purpose: Retrieves information about tracks from Apple Music.
	•	Functionality:
	•	Fetches track details such as ID, name, artist, album, genre, date added, and status.
	•	Supports filtering by a specific artist if provided as an argument.
	•	Formats the output in a structured manner for the Python script to parse.
	2.	update_property.applescript:
	•	Purpose: Updates specified properties (name, album, genre) of a track in Apple Music.
	•	Functionality:
	•	Takes a track ID, property name, and new property value as arguments.
	•	Updates the specified property of the given track.
	•	Returns a success or error message based on the operation outcome.

Location:
	•	Both scripts are stored in the directory specified by the apple_scripts_dir parameter in config.yaml.

Usage:
	•	The Python script music_genre_updater.py invokes these AppleScript scripts using the osascript command to perform necessary operations on the Apple Music library.

# Contributing

Contributions to the Music Genre Updater are welcome! To contribute, please follow these steps:
	1.	Fork the Repository:
Click the “Fork” button at the top-right corner of the repository page to create your own fork.
	2.	Clone Your Fork:
```bash
git clone https://github.com/yourusername/music-genre-updater.git
cd music-genre-updater
```

	3.	Create a New Branch:

```bash
git checkout -b feature/YourFeatureName
```

	4.	Make Your Changes:
Implement your feature or bug fix in your local branch.
	5.	Commit Your Changes:

```bash
git commit -m "Add feature: YourFeatureName"
```

	6.	Push to Your Fork:

```bash
git push origin feature/YourFeatureName
```


	7.	Create a Pull Request:
Navigate to your forked repository on GitHub and click the “Compare & pull request” button to submit your changes for review.

Please ensure your contributions adhere to the following guidelines:
	•	Follow the existing code style and conventions.
	•	Write clear and concise commit messages.
	•	Include relevant documentation or tests for your changes.

# License

This project is licensed under the MIT License. You are free to use, modify, and distribute this software as per the terms of the license.

# Contact

For any questions, suggestions, or support, please reach out:
	•	Author: Roman Borodavkin
	•	Email: roman.borodavkin@gmail.com
	•	GitHub: [@barad1tos](https://github.com/barad1tos)
	•	LinkedIn: [Roman Borodavkin](https://www.linkedin.com/in/barad1tos/)

Note: This project is intended for personal use. Before using the scripts, ensure you understand how they operate to prevent unintended changes to your Apple Music library.

# Troubleshooting

If you encounter issues while setting up or running the Music Genre Updater, consider the following troubleshooting steps:
	1.	Check Log Files:
	•	Review the log files specified in config.yaml and the plist file for error messages.
	2.	Verify Paths:
	•	Ensure all paths in config.yaml and the plist file are correct and accessible.
	3.	Permissions:
	•	Confirm that the script has the necessary permissions to read and write to the specified directories and files.
	4.	AppleScript Execution:
	•	Test the AppleScript scripts manually to ensure they function correctly.
	•	Open the Terminal and run:

```bash
osascript /path/to/fetch_tracks.applescript

osascript /path/to/fetch_tracks.applescript
```

	5.	Python Dependencies:
	•	Ensure all Python dependencies are installed correctly within your virtual environment.
	•	Reinstall dependencies if necessary:

```bash
pip install --upgrade --force-reinstall -r requirements.txt
```

	6.	Launch Agent Status:
	•	Verify that the launchctl agent is loaded and running:

```bash
launchctl list | grep com.barad1tos.MusicGenreUpdater
```

	•	If not running, reload the agent:

```bash
launchctl unload ~/Library/LaunchAgents/com.barad1tos.MusicGenreUpdater.plist
launchctl load ~/Library/LaunchAgents/com.barad1tos.MusicGenreUpdater.plist
```

	7.	Python Version:
	•	Ensure you are using Python 3.8 or higher:
```bash
python3 --version
```

# FAQ

Q1: Can I adjust the frequency of genre updates?

A: Yes, you can. You can adjust the StartInterval value in the com.barad1tos.MusicGenreUpdater.plist file to control how often the script runs (in seconds). In addition, the incremental_interval_minutes parameter in config.yaml controls the interval for incremental updates.

Q2: How do I add exceptions for specific artists or albums?

A: Modify the exceptions.The track_cleaning section in config.yaml should include the artist and album combinations you want to exclude from cleaning. For example:

Exceptions:
  track_cleaning:
    - artist: "Artist Name"
      album: "Album Name

Q3: What happens if the script fails to update a genre after several attempts?

A: The script will log an error message indicating the failure. It will attempt to update the genre again based on the max_retries and retry_delay_seconds settings in the config.yaml. If all retries fail, the track's genre will remain unchanged.

Q4: Is there a way to back up my music library before running the updater?

A: Yes and no. Apple Music syncs your changes almost instantly, so there's no way to prevent it. Even if you back up your library file and replace it with the current one after making unnecessary changes, Apple Music will still pull the changes from the cloud. It can still be useful if you are careful that the script does not corrupt your library.

Q5: How can I see the changes made by the script?

A: The script creates a changes_report.csv file as specified in config.yaml. This file contains details of all changes made during the update process, including artist, album, track name, old genre, new genre, and new track name. 

Q6: Can I run multiple instances of the script at the same time?

A: It is not recommended to run multiple instances of the script at the same time, as this can lead to race conditions or conflicting updates. Make sure that only one instance is running at a time, especially when using scheduled tasks such as launchctl.

Q7: How do I update the script to the latest version?

A: To update the script, pull the latest changes from the repository:

git pull origin main

Make sure you check for any configuration or dependency updates and adjust your setup accordingly.

Q8: Can I customize the keywords used to clean track and album names?

A: Yes, you can. You can modify the remaster_keywords and album_suffixes_to_remove in the cleaning section of config.yaml to include or exclude specific keywords based on your preferences.

Disclaimer: Always make sure you have backups of your music library before running automated scripts that modify your data. Use this tool at your own risk.

