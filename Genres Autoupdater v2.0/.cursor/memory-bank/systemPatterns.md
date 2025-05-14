# System Patterns & Conventions

## 1. Code style

- **Black** + **Isort** (150 characters)
- Comments in English, log messages in Ukrainian.

## 2. Architecture

mermaid
flowchart TD
CLI -->|parses args| Config[Config Loader]
Config --> DI[Dependency Injector]
DI --> Services
Services -->|async| Core[Genre Updater]
Core --> Reports[CSV Reporter]

## 3. Design Patterns

| Pattern                | Place in the code                  |
| ---------------------- | ---------------------------------- |
| Dependency Injection   | services/**init**.py               |
| Strategy (genre rules) | helpers/metadata.py                |
| Retry (Tenacity)       | services/\_external_api_service.py |
| Facade                 | music_genre_updater.py             |

## 4. Git / CI

    - main → release-tags
    - pre-commit hook: scripts/update_memory_bank.py
    - GitHub Actions (yet-to-add)
