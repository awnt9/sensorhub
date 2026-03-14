from unittest.mock import MagicMock


def test_health(client):
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_post_reading(client, mock_db):
    payload = {
        "device_id": "sensor-01",
        "location": "office",
        "temperature": 22.5,
        "humidity": 55.0,
        "co2": 420.0,
    }
    response = client.post("/readings", json=payload)
    assert response.status_code == 201
    assert response.json()["message"] == "Sensor data uploaded successfully"
    mock_db.upload_sensor_data.assert_called_once()


def test_get_readings(client, mock_db):
    mock_db.read_sensor_data.return_value = [
        {
            "_id": "507f1f77bcf86cd799439011",
            "device_id": "sensor-01",
            "location": "office",
            "temperature": 22.5,
            "humidity": 55.0,
            "co2": 420.0,
            "timestamp": "2026-03-14T10:00:00",
        }
    ]
    response = client.get("/readings")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["device_id"] == "sensor-01"


def test_get_readings_with_filters(client, mock_db):
    mock_db.read_sensor_data.return_value = []
    response = client.get("/readings?device_id=sensor-01&limit=10")
    assert response.status_code == 200
    mock_db.read_sensor_data.assert_called_once_with(device_id="sensor-01", max_records=10)


def test_get_stats_empty(client, mock_db):
    mock_db.read_sensor_data.return_value = []
    response = client.get("/readings/stats")
    assert response.status_code == 200
    assert response.json() == []


def test_get_stats(client, mock_db):
    mock_db.read_sensor_data.return_value = [
        {"device_id": "sensor-01", "location": "office", "temperature": 22.0, "humidity": 55.0, "co2": 400.0},
        {"device_id": "sensor-01", "location": "office", "temperature": 24.0, "humidity": 60.0, "co2": 500.0},
        {"device_id": "sensor-02", "location": "warehouse", "temperature": 18.0, "humidity": 70.0, "co2": 350.0},
    ]
    response = client.get("/readings/stats")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 2
    device_ids = {row["device_id"] for row in data}
    assert "sensor-01" in device_ids
    assert "sensor-02" in device_ids
    sensor01 = next(r for r in data if r["device_id"] == "sensor-01")
    assert sensor01["count"] == 2
    assert sensor01["avg_temperature"] == 23.0
    assert sensor01["max_co2"] == 500.0


def test_export_csv(client, mock_db):
    mock_db.read_sensor_data.return_value = [
        {"device_id": "sensor-01", "location": "office", "temperature": 22.5, "humidity": 55.0, "co2": 420.0},
    ]
    response = client.get("/export")
    assert response.status_code == 200
    assert response.headers["content-type"] == "text/csv; charset=utf-8"
    assert "sensor-01" in response.text


def test_generate_report_no_data(client, mock_db, mock_minio):
    mock_db.read_sensor_data_by_time.return_value = []
    response = client.post("/reports/generate?hour=2026-03-14T10:00:00")
    assert response.status_code == 404


def test_generate_report(client, mock_db, mock_minio):
    mock_db.read_sensor_data_by_time.return_value = [
        {"device_id": "sensor-01", "location": "office", "temperature": 22.5, "humidity": 55.0, "co2": 420.0},
    ]
    mock_minio.upload_csv.return_value = "2026-03-14/1000.csv"
    response = client.post("/reports/generate?hour=2026-03-14T10:00:00")
    assert response.status_code == 200
    data = response.json()
    assert "object_key" in data
    assert "link" in data


def test_list_reports(client, mock_db, mock_minio):
    mock_minio.list_reports.return_value = [
        {"name": "2026-03-14/1000.csv", "size": 1024, "last_modified": "2026-03-14T10:05:00"}
    ]
    response = client.get("/reports")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["name"] == "2026-03-14/1000.csv"


def test_get_report(client, mock_db, mock_minio):
    mock_minio.get_report.return_value = b"device_id,location\nsensor-01,office\n"
    response = client.get("/reports/2026-03-14/1000.csv")
    assert response.status_code == 200
    assert "sensor-01" in response.text


def test_get_report_not_found(client, mock_db, mock_minio):
    mock_minio.get_report.side_effect = Exception("not found")
    response = client.get("/reports/nonexistent.csv")
    assert response.status_code == 404
