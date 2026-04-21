import gc

import pyarrow as pa

from rlh import delta_io


def test_roundtrip_append_and_version(tmp_path):
    path = tmp_path / "t"
    delta_io.write_table(path, pa.table({"id": [1, 2], "v": ["a", "b"]}), mode="append")
    delta_io.write_table(path, pa.table({"id": [3], "v": ["c"]}), mode="append")
    assert delta_io.exists(path)
    assert delta_io.version(path) == 1
    tbl = delta_io.read_arrow(path)
    assert tbl.num_rows == 3
    assert len(delta_io.history(path)) == 2


def test_overwrite(tmp_path):
    path = tmp_path / "t"
    delta_io.write_table(path, pa.table({"id": [1, 2, 3]}), mode="append")
    delta_io.write_table(path, pa.table({"id": [9]}), mode="overwrite")
    assert delta_io.read_arrow(path).num_rows == 1


def test_exists_false_for_missing(tmp_path):
    assert delta_io.exists(tmp_path / "nope") is False


def test_no_lingering_handles():
    # read_arrow must not leak a DeltaTable (which would abort at exit).
    gc.collect()
    from deltalake import DeltaTable
    assert not any(isinstance(o, DeltaTable) for o in gc.get_objects())
