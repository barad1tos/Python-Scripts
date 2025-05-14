# Tech Context

## 1. Runtime / OS

- **macOS Sonoma 15.5** (Apple Silicon)
- Homebrew 4.x (package management)
- Python 3.12 (via pyenv + .venv)
- Bash 5.2 (scripts / Git hooks)
- Terraform 1.8 (CI part, not yet used directly in the script)

## 2. Python dependencies

| Package  | Version | Purpose                        |
| -------- | ------- | ------------------------------ |
| PyYAML   | ≥ 6.0   | parsing `.yaml' configurations |
| tenacity | ≥ 8.0   | retry logic for APIs           |
| rich     | ≥ 13.7  | colored CLI output             |
| ...      | ...     | ...                            |

> **📝 TODO:** add libs after `pip freeze > requirements.txt`.

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
- **Last.fm API** (road-map) - metadata collection
- Local cache in csv (`services/_cache_service.py`)

> **📝 TODO:** specify the versions of external APIs and keys (free of charge - ENV vars).
