from unittest.mock import MagicMock, patch

from src import db


def make_mock_connection():
    connection = MagicMock()
    cursor = MagicMock()
    connection.cursor.return_value.__enter__.return_value = cursor
    return connection, cursor


def test_insert_readings_returns_zero_for_empty_list():
    connection, cursor = make_mock_connection()

    result = db.insert_readings(connection, [])

    assert result == 0
    cursor.execute.assert_not_called()
    connection.commit.assert_not_called()


def test_insert_readings_batches_and_commits():
    connection, cursor = make_mock_connection()
    readings = [
        {"customer_id": "CUST-001", "timestamp": "2026-01-01T00:00:00+00:00", "heart_rate": 72},
        {
            "customer_id": "CUST-002",
            "timestamp": "2026-01-01T00:00:01+00:00",
            "heart_rate": 250,
            "is_anomalous": True,
        },
    ]

    with patch("src.db.psycopg2.extras.execute_values") as mock_execute_values:
        inserted = db.insert_readings(connection, readings)

    assert inserted == 2
    mock_execute_values.assert_called_once()
    passed_rows = mock_execute_values.call_args[0][2]
    assert passed_rows == [
        ("CUST-001", "2026-01-01T00:00:00+00:00", 72, False),
        ("CUST-002", "2026-01-01T00:00:01+00:00", 250, True),
    ]
    connection.commit.assert_called_once()


def test_fetch_recent_readings_executes_query_with_params():
    connection, cursor = make_mock_connection()
    cursor.fetchall.return_value = [("CUST-001", "2026-01-01T00:00:00+00:00", 72, False)]

    result = db.fetch_recent_readings(connection, "CUST-001", limit=5)

    cursor.execute.assert_called_once()
    _, params = cursor.execute.call_args[0]
    assert params == ("CUST-001", 5)
    assert result == [("CUST-001", "2026-01-01T00:00:00+00:00", 72, False)]


def test_apply_schema_executes_ddl_and_commits(tmp_path):
    connection, cursor = make_mock_connection()
    schema_file = tmp_path / "schema.sql"
    schema_file.write_text("CREATE TABLE example (id INT);")

    db.apply_schema(connection, schema_file=schema_file)

    cursor.execute.assert_called_once_with("CREATE TABLE example (id INT);")
    connection.commit.assert_called_once()
