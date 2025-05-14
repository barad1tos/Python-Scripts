# Tech Context

## 1. Runtime / OS

- **macOS Sonoma 15.5** (Apple Silicon)
- Homebrew 4.x (package management)
- Python 3.12 (via pyenv + .venv)
- Bash 5.2 (scripts / Git hooks)
- Terraform 1.8 (CI part, not yet used directly in the script)

## 2. Python dependencies

| Package  | Version | Purpose                             |
| -------- | ------- | ----------------------------------- |
| PyYAML   | ≥ 6.0   | parsing `.yaml' configurations      |
| aiohttp  | ?       | Asynchronous HTTP client (for APIs) |
| tenacity | ≥ 8.0   | Retry logic for APIs calls          |
| rich     | ≥ 13.7  | Colored CLI output                  |

> **📝 TODO:** Verify this list against actual project dependencies (e.g., using `pip freeze` or checking setup files) and specify exact versions if necessary.

## 3. Directories

```shell
project/
├─ .cursor/               # AI configuration, memory bank, rules, and related scripts
│  ├─ memory-bank/        # Markdown files storing project context, brief, progress, etc.
│  ├─ rules/              # Custom rules and instructions for the AI
│  ├─ scripts/            # Scripts used internally by the AI tools (e.g., update_memory_bank.py)
├─ diagnostics/           # Scripts related to diagnostics or debugging output
├─ services/              # Service layer modules implementing specific functionalities
│  ├─ applescript_client.py # Handles communication with Apple Music via AppleScript
│  ├─ cache_service.py    # Manages in-memory and persistent caching (e.g., album years)
│  ├─ dependencies_service.py # Dependency injection container for managing service instances
│  ├─ external_api_service.py # Interfaces with external music databases (MusicBrainz, Discogs, Last.fm)
│  ├─ pending_verification.py # Manages albums marked for future year re-verification
├─ utils/                 # Utility functions and modules used across the project
│  ├─ analytics.py        # Performance tracking and reporting framework
│  ├─ config.py           # Configuration loading and validation
│  ├─ dry_run.py          # Contains logic for simulating script execution without applying changes
│  ├─ logger.py           # Custom logging system setup and handlers
│  ├─ metadata.py         # Utility functions for processing track metadata (parsing, cleaning, genre determination)
│  ├─ metadata_helpers.py # (Likely deprecated or supporting) Additional metadata processing helpers
│  ├─ reports.py          # Functions for generating and saving various reports (CSV, HTML)
├─ .cache_ggshield        # Cache file for GitGuardian secret detection tool
├─ .gitguardian.yaml      # Configuration file for GitGuardian secret detection tool
├─ .gitignore             # Specifies intentionally untracked files that Git should ignore
├─ .python-version        # File specifying the Python version to be used (e.g., by pyenv)
├─ README.md              # Project overview, installation instructions, usage, architecture documentation
├─ music_genre_updater.py # Main script - entry point, argument parsing, orchestrates the update process
├─ my-config.yaml         # Main configuration file for the script settings and API keys (local overrides)
├─ pyproject.toml         # Project configuration for build tools, linters, formatters (e.g., Black, isort)
├─ requirements.txt       # List of Python dependencies required by the project
└─ temp/                  # Temporary directory (often used during testing or development)
   └─ Genres Autoupdater/ # (Example/Placeholder) Temporary sub-directory
```

## 4. Integrations

- **Music.app** via AppleScript to read/write tags
- **External Music APIs:**
  - **MusicBrainz API:** Used for retrieving album year metadata. Configuration for this API (including application name and contact email) is loaded from `my-config.yaml`.
  - **Discogs API:** Used as a fallback or alternative source for album year metadata. The API token is loaded from `my-config.yaml`.
- **Last.fm API** (road-map) - metadata collection. The API key is loaded from `my-config.yaml` if used.
- Local cache in csv (`services/_cache_service.py`)

> **📝 TODO:** Verify specific API versions targeted/used if relevant. Ensure API keys/tokens are securely handled, preferably loaded from environment variables rather than being directly in `my-config.yaml` if it's committed to version control.

## 4. Files

This section provides a description of the key files within the project structure.

- **`.cursor/memory-bank/activeContext.md`**: Stores the current active context and task details for the AI agent.
- **`.cursor/memory-bank/projectBrief.md`**: Contains the overall project goals, scope, and requirements.
- **`.cursor/memory-bank/progress.md`**: Tracks the progress of tasks and milestones.
- **`.cursor/memory-bank/systemPatterns.md`**: Documents established system patterns and architectural decisions.
- **`.cursor/memory-bank/techContext.md`**: Provides technical context about the codebase, including directory structure and file descriptions (this file).
- **`.cursor/rules/isolation_rules/...mdc`**: Various rule files used by the AI agent to guide its behavior and workflow within the "isolation" framework.
- **`.cursor/rules/memory-bank-always.mdc`**: A core rule requiring the AI to load Memory Bank files.
- **`.cursor/scripts/update_memory_bank.py`**: Script likely used by the AI environment to manage or update Memory Bank content.
- **`csv/cache_albums.csv`**: Persistent cache for storing determined album years.
- **`csv/changes_report.csv`**: Logs all modifications made to the Music library during script execution.
- **`csv/dry_run_combined.csv`**: Report combining simulated changes from a dry run.
- **`csv/pending_year_verification.csv`**: Lists albums that require future re-verification of their release year.
- **`csv/track_list.csv`**: The main database file storing details of all processed tracks, including original and updated metadata.
- **`main/last_db_verify.log`**: Records the timestamp of the last database verification run.
- **`main/main.log`**: The primary log file for general script execution information.
- **`main/year_changes.log`**: Logs specific details related to album year update processes.
- **`analytics/analytics.log`**: Log file specifically for performance analytics data.
- **`last_incremental_run.log`**: Records the timestamp of the last incremental update run.
- **`services/applescript_client.py`**: Module for interacting with the Music.app via AppleScript commands.
- **`services/cache_service.py`**: Implements the multi-tiered caching mechanism for various data types.
- **`services/dependencies_service.py`**: Sets up and manages the dependency injection container.
- **`services/external_api_service.py`**: Handles fetching data from external music APIs (MusicBrainz, Discogs, Last.fm).
- **`services/pending_verification.py`**: Service for managing the list and logic for albums needing pending year verification.
- **`utils/analytics.py`**: Defines the decorators and logic for performance tracking and reporting.
- **`utils/config.py`**: Contains the schema and function for loading and validating the project configuration.
- **`utils/dry_run.py`**: Module housing the logic for simulating the script's operations in dry run mode.
- **`utils/logger.py`**: Configures and provides the custom logging system, including handlers and formatters.
- **`utils/metadata.py`**: Provides core utility functions for parsing, cleaning, and analyzing track metadata.
- **`utils/metadata_helpers.py`**: (Likely supplementary or older utilities) Additional functions related to metadata processing.
- **`utils/reports.py`**: Contains functions for generating, saving, and managing various output reports (CSV, HTML).
- **`.gitguardian.yaml`**: Configuration file for the GitGuardian security scanning tool.
- **`.gitignore`**: Specifies which files and directories should be ignored by Git.
- **`.python-version`**: Specifies the Python version for tools like `pyenv`.
- **`README.md`**: The main project documentation, including description, features, installation, usage, and architecture overview.
- **`music_genre_updater.py`**: The main executable script that orchestrates the entire update process.
- **`my-config.yaml`**: The primary configuration file for the project's runtime settings, API keys, paths, etc.
- **`pyproject.toml`**: Configuration file used by modern Python build tools and potentially linters/formatters.
- **`requirements.txt`**: Lists the required Python packages for the project.
