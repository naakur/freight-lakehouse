"""
Static check (no Spark, no Databricks): every pipeline setting the code reads must be defined in
resources/pipelines.yml for THAT pipeline. This catches 'SQL config "freight.xxx" cannot be found'
before you deploy.

How: for each pipeline, find its source files (the globs), look for helper calls like table() or
stream_path() and explicit conf("freight.x") reads, and compare with the pipeline's `configuration`.
"""
import pathlib
import re

import pytest
import yaml

ROOT = pathlib.Path(__file__).resolve().parents[1]

# which pipeline settings each helper in utilities/config.py needs
HELPER_KEYS = {
    "table": {"freight.catalog"},
    "landing_path": {"freight.landing_root"},
    "stream_path": {"freight.landing_root"},
}
# values the ${var.stream_source} placeholder can take
STREAM_SOURCES = ["files", "kafka"]


def pipelines():
    return yaml.safe_load((ROOT / "resources" / "pipelines.yml").read_text())["resources"]["pipelines"]


def source_files(pipeline, stream_source):
    files = []
    for lib in pipeline["libraries"]:
        pattern = lib["glob"]["include"].replace("${var.stream_source}", stream_source)
        folder = (ROOT / "resources" / pattern).resolve().parent            # glob ends with /**
        files += list(folder.rglob("*.py"))
    return files


def keys_needed(files):
    needed = set()
    for f in files:
        text = f.read_text()
        for helper, keys in HELPER_KEYS.items():
            if re.search(rf"\b{helper}\(", text) and "def " + helper not in text:
                needed |= keys
        needed |= set(re.findall(r"""conf\(\s*["'](freight\.[\w.]+)["']""", text))
    return needed


@pytest.mark.parametrize("stream_source", STREAM_SOURCES)
@pytest.mark.parametrize("name", list(pipelines()))
def test_pipeline_defines_every_setting_its_code_reads(name, stream_source):
    pipeline = pipelines()[name]
    defined = set(pipeline["configuration"])
    files = source_files(pipeline, stream_source)
    assert files, f"no source files found for {name}"
    missing = keys_needed(files) - defined
    assert not missing, f"{name} ({stream_source}): code reads {sorted(missing)} but the pipeline does not define them"
