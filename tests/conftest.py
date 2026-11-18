import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))  # tests import each other's helpers (faults, flower_stack)
