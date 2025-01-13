import pyarrow as pa
import datetime
from instructure_dap_helpers.tidy import format_instructure_data


def test_format_instructure_data():
    t = pa.Table.from_arrays(arrays=[[2, 4, 5, 100],
                                     ["Flamingo", "Horse", "Brittle stars", "Centipede"]],
                             names=['n_legs.GOOB1', 'animals.GOOB2'])
    f = format_instructure_data(t)
    assert "GOOB1" in f.column_names and "GOOB2" in f.column_names


def test_format_instructure_data_ma():
    t = pa.Table.from_arrays(arrays=[[2, 4, 5, 100],
                                     ["Flamingo", "Horse", "Brittle stars", "Centipede"],
                                     ["Flamingo", "Horse", "Brittle stars", "Centipede"]],
                             names=['n_legs.GOOB1', 'animals.GOOB2', 'meta.action'])
    t = t.cast(t.schema.set(2, pa.field("meta.action", pa.string(), nullable=False)))
    f = format_instructure_data(t)
    assert f.field(2).nullable


def test_format_instructure_data_nested():
    meta = pa.array([
        {"action": "I", "ts": datetime.datetime.now()},
        {"action": "D", "ts": datetime.datetime.now()},
        {"action": "U", "ts": datetime.datetime.now()}
    ])
    key = pa.array([
        {"id": 1},
        {"id": 2},
        {"id": 3},
    ])
    values = pa.array([
        {"class_name": "science", "time": "mornin'"},
        {"class_name": "math", "time": "afternoon"},
        {"class_name": "readin'", "time": "evenin'"},
    ])
    t = pa.Table.from_arrays([meta, key, values],
                             names=["meta", "key", "values"])
    f = format_instructure_data(t)
    print(f.column_names)
    assert f.field(f.schema.get_field_index("action")).nullable
    assert f.num_rows == 3
    assert sorted(f.column_names) == ["action", "class_name", "id", "time", "ts"]
