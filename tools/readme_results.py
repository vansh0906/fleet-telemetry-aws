#!/usr/bin/env python3
"""Copies results.txt (output of `python run_all.py | tee results.txt`) into the README results block."""
import re
import sys
from pathlib import Path

root = Path(__file__).resolve().parent.parent
results = (root / "results.txt").read_text().strip()
readme = root / "README.md"
text = readme.read_text()
new, n = re.subn(r"<!-- results:start -->.*?<!-- results:end -->",
                 "<!-- results:start -->\n```text\n" + results + "\n```\n<!-- results:end -->", text, flags=re.S)
if n != 1:
    sys.exit("results markers not found in README.md")
readme.write_text(new)
print("README.md updated")
