import pytest

from src.utils import extract_run_name
from src.utils import make_model_filename


def test_roundtrip():
    filename = make_model_filename("pure_bolt", 5_000_000, 8, 1.4128)
    assert extract_run_name(filename) == "pure_bolt"


def test_make_model_filename():
    assert make_model_filename("pure_bolt", 5_000_000, 8, 1.4128) == "pure_bolt_model_t5M_ep8_v1d4128.pt"
    assert make_model_filename("vivid_dragon", 47_000_000, 12, 0.9461) == "vivid_dragon_model_t47M_ep12_v0d9461.pt"


def test_extract_run_name():
    assert extract_run_name("pure_bolt_model_t5M_ep8_v1d4128.pt") == "pure_bolt"
    assert extract_run_name("/some/path/vivid_dragon_model_t47M_ep12_v0d9461.pt") == "vivid_dragon"


def test_extract_run_name_invalid():
    with pytest.raises(ValueError):
        extract_run_name("pure_bolt_t5M_ep8_v1d4128.pt")  # missing model_ prefix
    with pytest.raises(ValueError):
        extract_run_name("pure_bolt_model_t5M_ep8.pt")  # missing v field
    with pytest.raises(ValueError):
        extract_run_name("pure_bolt_model_t5M_v1d4128.pt")  # missing ep field
    with pytest.raises(ValueError):
        extract_run_name("garbage.pt")
