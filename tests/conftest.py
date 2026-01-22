import sys
from pathlib import Path

# Add src directory to path so tests can import miniotel
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
