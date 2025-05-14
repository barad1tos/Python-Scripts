# Active Context

## 2025-05-14T22:08:09

```diff
# ⇢ Changed files
# Genres Autoupdater v2.0/.cursor/memory-bank/activeContext.md
# Genres Autoupdater v2.0/.cursor/rules/memory-bank-always.mdc

diff --git a/Genres Autoupdater v2.0/.cursor/memory-bank/activeContext.md b/Genres Autoupdater v2.0/.cursor/memory-bank/activeContext.md
index c4b1223..dbd6aae 100644
--- a/Genres Autoupdater v2.0/.cursor/memory-bank/activeContext.md
+++ b/Genres Autoupdater v2.0/.cursor/memory-bank/activeContext.md
@@ -1,164 +1,7 @@
 # Active Context

-`````
-## 2025-05-14T21:12:29
-
-### Changed files
-
-- Genres Autoupdater v2.0/.cursor/memory-bank/activeContext.md
-- Genres Autoupdater v2.0/.cursor/scripts/update_memory_bank.py
-
-````diff
-diff --git a/Genres Autoupdater v2.0/.cursor/memory-bank/activeContext.md b/Genres Autoupdater v2.0/.cursor/memory-bank/activeContext.md
-index 9b19b09..3c8805a 100644
---- a/Genres Autoupdater v2.0/.cursor/memory-bank/activeContext.md
-+++ b/Genres Autoupdater v

diff --git a/Genres Autoupdater v2.0/.cursor/rules/memory-bank-always.mdc b/Genres Autoupdater v2.0/.cursor/rules/memory-bank-always.mdc
index cd0edd9..ca5bc1c 100644
--- a/Genres Autoupdater v2.0/.cursor/rules/memory-bank-always.mdc
+++ b/Genres Autoupdater v2.0/.cursor/rules/memory-bank-always.mdc
@@ -2,13 +2,13 @@
 description:
 globs:
 alwaysApply: true
 ---
 ---
-description: Persistent Memory Bank
+description: Memory Bank (always)
 alwaysApply: true
 globs:
-  - ".cursor/memory-bank/**/*.md"
+  - memory-bank/*.md
 ---

-# IMPORTANT
-Read every file in `.cursor/memory-bank/` **before** answering.
\ No newline at end of file
+# READ BEFORE ANSWERING
+Load every .md in memory-bank/ before replying.
\ No newline at end of file

```

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
