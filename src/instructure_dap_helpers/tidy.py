import pyarrow as pa


def format_instructure_data(table: pa.Table) -> pa.Table:
    """
    Formats an Apache Arrow Table of instructure data to a more tidy format -- this can be used to stream instructure incremental loads to an iceberg database:
    1. Flattens the table.
    2. Casts the 'meta.action' field to a nullable string type if it exists. This will avoid schema conflict in future updates
    3. Renames columns by removing the prefix (meta/values/key).

    Args:
        table (pa.Table): The input Apache Arrow Table.

    Returns:
        pa.Table: The formatted Apache Arrow Table.
    """
    table = table.flatten()

    if table.schema.get_field_index("meta.action") > -1:  # -1 if it doesn't exist
        table = table.cast(table.schema.set(table.schema.get_field_index("meta.action"),
                                            pa.field("meta.action", pa.string(), nullable=True)))

    table = table.rename_columns([name.split(".")[1] for name in table.column_names])

    return table
