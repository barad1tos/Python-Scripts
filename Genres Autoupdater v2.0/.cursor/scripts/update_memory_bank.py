#!/usr/bin/env python3
"""
Summarise git diff and prepend it to .cursor/memory-bank/activeContext.md
"""
import datetime
import pathlib
import shutil
import subprocess  # trunk-ignore(bandit/B404)

ROOT = pathlib.Path(__file__).resolve().parents[2]  # project root (two levels up from .cursor/scripts)
MB = ROOT / ".cursor" / "memory-bank" / "activeContext.md"

git_path = shutil.which("git") or "git"
# trunk-ignore(bandit/B603)
diff = subprocess.run([git_path, "diff", "--staged"], capture_output=True, text=True, check=False).stdout

if diff:
    stamp = datetime.datetime.now().isoformat(timespec="seconds")
    snippet = f"## {stamp}\n\n" "```diff\n" f"{diff[:1500]}\n" "```\n\n"

    header = "# Active Context\n\n"

    # Read existing content (if any)
    existing = MB.read_text() if MB.exists() else ""

    # Strip an existing header (to avoid duplicates)
    if existing.startswith(header):
        existing = existing[len(header) :]  # noqa: E203

    # Compose new content
    new_content = header + snippet + existing

    MB.write_text(new_content)
