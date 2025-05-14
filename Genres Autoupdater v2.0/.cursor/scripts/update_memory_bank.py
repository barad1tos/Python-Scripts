#!/usr/bin/env python3
"""
Summarise git diff and prepend it to .cursor/memory‑bank/activeContext.md
"""
import datetime
import shutil
import subprocess  # trunk-ignore(bandit/B404)
from pathlib import Path

# Resolve project root (two levels up from .cursor/scripts/)
ROOT = Path(__file__).parent.parent.parent
MB = ROOT / ".cursor" / "memory-bank" / "activeContext.md"

git_path = shutil.which("git") or "git"

# Collect staged files (exclude memory‑bank itself)
files_out = subprocess.run(  # trunk-ignore(bandit/B603)
    [git_path, "diff", "--staged", "--name-only", "--diff-filter=ACMRT"],
    capture_output=True,
    text=True,
    check=False,
).stdout
files = [f for f in files_out.splitlines() if f and not f.startswith(".cursor/memory-bank")]

if not files:
    raise SystemExit(0)  # No relevant changes staged

stamp = datetime.datetime.now().isoformat(timespec="seconds")
file_list_md = "\n".join(f"- {f}" for f in files)

diff_blocks = []
for path in files:
    # Ensure path stays within repository (basic safety check)
    safe_path = (ROOT / path).resolve()
    if not str(safe_path).startswith(str(ROOT.resolve())):
        continue
    diff_text = subprocess.run(  # trunk-ignore(bandit/B603)
        [git_path, "diff", "--staged", "--unified=5", "--", str(safe_path.relative_to(ROOT))],
        capture_output=True,
        text=True,
        check=False,
    ).stdout
    diff_blocks.append(f"```diff\n{diff_text[:800]}\n```\n")

snippet = f"## {stamp}\n\n" "### Changed files\n\n" f"{file_list_md}\n\n" + "\n".join(diff_blocks)

header = "# Active Context\n\n"
existing = MB.read_text() if MB.exists() else ""

# Remove duplicate header if present
if existing.startswith(header):
    existing = existing[len(header) :]  # noqa: E203

new_content = header + snippet + existing

# Trim trailing whitespace per line and ensure single ending newline
clean_content = "\n".join(line.rstrip() for line in new_content.splitlines()) + "\n"

# Add whitespace to the end of the file
clean_content += "\n"

MB.parent.mkdir(parents=True, exist_ok=True)
MB.write_text(clean_content)
