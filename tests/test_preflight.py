"""Run with:  pytest tests/    (no Spark needed)"""
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src" / "freight_etl"))

from utilities.preflight import check_file
from utilities.schemas import SOURCES

GOOD_HEADER = ",".join(SOURCES["gl_codes"])


def test_good_file_passes():
    assert check_file("gl_codes", "gl.csv", 100, f"{GOOD_HEADER}\n6100,Freight\n") is None


def test_empty_file():
    assert check_file("gl_codes", "gl.csv", 0, "") == "EMPTY_FILE"


def test_wrong_extension():
    assert check_file("gl_codes", "gl.txt", 50, f"{GOOD_HEADER}\n6100,x\n") == "NOT_A_CSV"


def test_header_mismatch():
    assert check_file("gl_codes", "gl.csv", 50, "wrong,columns\n1,2\n") == "HEADER_MISMATCH"


def test_header_only_file():
    assert check_file("gl_codes", "gl.csv", 20, f"{GOOD_HEADER}\n") == "NO_DATA_ROWS"


def test_excel_bom_is_tolerated():
    assert check_file("gl_codes", "gl.csv", 100, "\ufeff" + f"{GOOD_HEADER}\n6100,x\n") is None
