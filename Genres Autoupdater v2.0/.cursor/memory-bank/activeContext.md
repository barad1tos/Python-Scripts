# Active Context

```diff
diff --git a/Genres Autoupdater v2.0/.cursor/scripts/update_memory_bank.py b/Genres Autoupdater v2.0/.cursor/scripts/update_memory_bank.py
index 0045f6b..25fcb91 100755
--- a/Genres Autoupdater v2.0/.cursor/scripts/update_memory_bank.py
+++ b/Genres Autoupdater v2.0/.cursor/scripts/update_memory_bank.py
@@ -1,33 +1,53 @@
 #!/usr/bin/env python3
 """
 Summarise git diff and prepend it to .cursor/memory-bank/activeContext.md
 """
 import datetime
-import pathlib
 import shutil
 import subprocess  # trunk-ignore(bandit/B404)
+from pathlib import Path

-ROOT = pathlib.Path(__file__).resolve().parents[2]  # project root (two levels up from .cursor/scripts)
+ROOT = Path(__file__).parent.parent.parent  # project root (two levels up from .cursor/scripts)
 MB = ROOT / ".cursor" / "memory-bank" /
```
