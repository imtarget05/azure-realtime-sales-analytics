import sys
import os

# Add repo root to sys.path so that top-level packages (scripts, config, ml, etc.)
# are importable from tests without requiring installation.
sys.path.insert(0, os.path.dirname(__file__))
