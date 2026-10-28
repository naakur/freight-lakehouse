"""
File-level validation rules used by notebooks/01_preflight_validate_files.py.
Pure Python (no Spark) so it can be unit tested in a second.
"""
from utilities.schemas import SOURCES


def check_file(source: str, name: str, size: int, head_text: str):
    """
    Look at ONE file (name, size in bytes, and its first few KB of text).
    Return None if the file is fine, otherwise a short reason string.
    """
    if size == 0:
        return "EMPTY_FILE"
    if not name.lower().endswith(".csv"):
        return "NOT_A_CSV"

    lines = head_text.lstrip("\ufeff").splitlines()      # lstrip removes an Excel BOM if present
    header = [c.strip() for c in lines[0].split(",")] if lines else []
    if header != SOURCES[source]:
        return "HEADER_MISMATCH"
    if len(lines) < 2:
        return "NO_DATA_ROWS"
    return None
