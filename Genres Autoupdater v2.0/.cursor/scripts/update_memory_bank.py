#!/usr/bin/env python3
"""
Summarise git diff and prepend it to .cursor/memory-bank/activeContext.md
"""
import datetime
import pathlib
import shutil
import subprocess  # trunk-ignore(bandit/B404)
import textwrap

ROOT = pathlib.Path(__file__).resolve().parents[2]  # project root (two levels up from .cursor/scripts)
MB = ROOT / ".cursor" / "memory-bank" / "activeContext.md"

git_path = shutil.which("git") or "git"
# trunk-ignore(bandit/B603)
diff = subprocess.run([git_path, "diff", "--staged"], capture_output=True, text=True, check=False).stdout

if diff:
    stamp = datetime.datetime.now().isoformat(timespec="seconds")
    snippet = textwrap.dedent(
        f"""\
    ## {stamp}
    ```diff
    {diff[:1500]}
    ```
    """
    )
    MB.parent.mkdir(parents=True, exist_ok=True)
    MB.write_text(snippet + "\n\n" + MB.read_text() if MB.exists() else snippet)
