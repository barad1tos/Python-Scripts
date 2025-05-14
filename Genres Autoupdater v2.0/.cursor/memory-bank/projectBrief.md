# Genres Autoupdater - Project Brief

**Version:** 3.0
**Last update:** 2025-05-14

## 1. One-liner

Automated Music.app library manager: harmonizes genres, cleans tags, updates release year, and generates reports.

## 2. Problem

- Manually correcting tags in Music.app → a lot of time and mistakes.
- There is no single source of truth for the "correct" genres.
- There are many messages like "No results from Last.fm, Discogs, etc." in the console.

## 3. Solution

- A **Python 3.12+** script with asyncio, a DI container, and a service layer.
- YAML-config + CLI flags → flexible customization.
- Reports in CSV/Markdown.

## 4. Scope / Out-of-scope

| In scope              | Out of scope                |
| --------------------- | --------------------------- |
| Tag generation/update | Music download              |
| iTunes XML parsing    | Automatic artwork           |
| CSV + MD reports      | Machine learning for genres |

## 5. Readiness criteria

- The `python music_genre_updater.py --dry-run` command completes without errors.
- 90% of tracks get a valid genre from the base dictionary.

> **📝 TODO (fill in the blank):** add specific KPIs and deadlines.

- Deal with errors in the console
- Implement incremental work when determining the year through API
- Embed the implementation of the previous paragraph in all existing procedures and functions
