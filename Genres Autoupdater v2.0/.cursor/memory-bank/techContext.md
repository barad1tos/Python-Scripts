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
├─ .cursor/               # rules + memory‑bank + scripts
├─ helpers/, utils/       # utility modules
├─ services/              # DI‑level (AppleScript, DB)
├─ music_genre_updater.py # main script
└─ config.yaml            # main settings
```

## 4. Integrations

- **Music.app** via AppleScript to read/write tags
- **External Music APIs:**
  - **MusicBrainz API:** Used for retrieving album year metadata. Configuration for this API (including application name and contact email) is loaded from `my-config.yaml`.
  - **Discogs API:** Used as a fallback or alternative source for album year metadata. The API token is loaded from `my-config.yaml`.
- **Last.fm API** (road-map) - metadata collection. The API key is loaded from `my-config.yaml` if used.
- Local cache in csv (`services/_cache_service.py`)

> **📝 TODO:** Verify specific API versions targeted/used if relevant. Ensure API keys/tokens are securely handled, preferably loaded from environment variables rather than being directly in `my-config.yaml` if it's committed to version control.
