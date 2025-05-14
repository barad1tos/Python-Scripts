#!/usr/bin/env python3
"""
Summarise staged changes and prepend them to
.cursor/memory‑bank/activeContext.md in a lint‑friendly Markdown format.
"""

import datetime
import shutil
import subprocess  # trunk-ignore(bandit/B404)

from pathlib import Path


# ───────────────────────────── Config ──────────────────────────────
ROOT = Path(__file__).parent.parent.parent  # <repo>/
MB = ROOT / ".cursor" / "memory-bank" / "activeContext.md"
git = shutil.which("git") or "git"  # full path to git


# ─────────────────────── Collect staged file list ──────────────────
files_out = subprocess.run(  # trunk-ignore(bandit/B603)
    [git, "diff", "--staged", "--name-only", "--diff-filter=ACMRT"],
    capture_output=True,
    text=True,
    check=False,
).stdout

files = [f for f in files_out.splitlines() if f and not f.startswith(".cursor/memory-bank")]

if not files:
    raise SystemExit(0)  # nothing to log


# ─────────────────────── Build fenced diff snippet ─────────────────
stamp = datetime.datetime.now().isoformat(timespec="seconds")

# Header for this entry
entry_header = f"## {stamp}\n\n"

# Fence begins: list files as commented lines, then diffs
fence_lines: list[str] = ["```diff", "# ⇢ Changed files"]
fence_lines.extend(f"# {p}" for p in files)
fence_lines.append("")  # blank line before actual diffs

for path in files:
    diff_text = subprocess.run(  # trunk-ignore(bandit/B603)
        [git, "diff", "--staged", "--unified=5", "--", path],
        capture_output=True,
        text=True,
        check=False,
    ).stdout
    fence_lines.append(diff_text[:800].rstrip())
    fence_lines.append("")  # blank line between file diffs

fence_lines.append("```")

snippet = entry_header + "\n".join(fence_lines) + "\n\n"


# ─────────────────────────── Write to file ─────────────────────────
HEADER = "# Active Context\n\n"
existing = MB.read_text() if MB.exists() else ""

# Avoid duplicate top header
if existing.startswith(HEADER):
    existing = existing[len(HEADER) :]  # noqa: E203

new_content = HEADER + snippet + existing

# Strip trailing spaces per line, ensure exactly one trailing newline
clean = "\n".join(line.rstrip() for line in new_content.splitlines()) + "\n"

MB.parent.mkdir(parents=True, exist_ok=True)
MB.write_text(clean)
