from pyspark.sql import SparkSession


def assert_table_exists(
    spark: SparkSession,
    table_name: str,
    *,
    require_readable: bool = True,
    require_non_empty: bool = True,
) -> None:
    """
    Assert that a Spark table exists, and optionally that it is readable
    and/or non-empty.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.
    table_name : str
        Fully-qualified table name (catalog.schema.table or schema.table).
    require_readable : bool, default True
        If True, attempts a lightweight read to ensure the table is resolvable.
    require_non_empty : bool, default False
        If True, asserts that the table has at least one row.

    Raises
    ------
    RuntimeError
        If any required condition is not met.
    """

    if not spark.catalog.tableExists(table_name):
        raise RuntimeError(f"Table does not exist: {table_name}")
    print(f"Table exists: {table_name}")
    if require_readable:
        try:
            spark.table(table_name).limit(1).collect()
        except Exception as e:
            raise RuntimeError(
                f"Table exists but is not readable: {table_name}"
            ) from e
        print(f"    Table is readable.")

    if require_non_empty:
        if spark.table(table_name).limit(1).count() == 0:
            raise RuntimeError(
                f"Table exists but is empty: {table_name}"
            )
        print(f"    Table is non-empty.")