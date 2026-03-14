import csv
import io

HOUR = "2026-03-14T10:00:00"

READINGS = [
    {"device_id": "sensor-01", "location": "office",    "temperature": 22.5, "humidity": 55.0, "co2": 420.0, "timestamp": "2026-03-14T10:15:00"},
    {"device_id": "sensor-01", "location": "office",    "temperature": 23.0, "humidity": 57.0, "co2": 450.0, "timestamp": "2026-03-14T10:30:00"},
    {"device_id": "sensor-02", "location": "warehouse", "temperature": 18.0, "humidity": 70.0, "co2": 380.0, "timestamp": "2026-03-14T10:10:00"},
    {"device_id": "sensor-02", "location": "warehouse", "temperature": 19.0, "humidity": 72.0, "co2": 400.0, "timestamp": "2026-03-14T10:45:00"},
    {"device_id": "sensor-03", "location": "meeting",   "temperature": 24.0, "humidity": 50.0, "co2": 800.0, "timestamp": "2026-03-14T10:55:00"},
]


def test_full_report_flow(client):
    # 1. POST readings
    for reading in READINGS:
        r = client.post("/readings", json=reading)
        assert r.status_code == 201, r.text

    # 2. GET /readings — todos llegaron
    r = client.get("/readings")
    assert r.status_code == 200
    assert len(r.json()) == len(READINGS)

    # 3. GET /readings con filtro por device
    r = client.get("/readings?device_id=sensor-01")
    assert r.status_code == 200
    assert len(r.json()) == 2
    assert all(row["device_id"] == "sensor-01" for row in r.json())

    # 4. GET /readings/stats — agregaciones correctas
    r = client.get("/readings/stats")
    assert r.status_code == 200
    stats = {row["device_id"]: row for row in r.json()}

    assert stats["sensor-01"]["count"] == 2
    assert stats["sensor-01"]["avg_temperature"] == 22.75
    assert stats["sensor-01"]["max_co2"] == 450.0

    assert stats["sensor-02"]["count"] == 2
    assert stats["sensor-03"]["max_co2"] == 800.0

    # 5. GET /export — CSV con todos los datos
    r = client.get("/export")
    assert r.status_code == 200
    assert "text/csv" in r.headers["content-type"]
    rows = list(csv.DictReader(io.StringIO(r.text)))
    assert len(rows) == len(READINGS)

    # 6. POST /reports/generate — genera el reporte en MinIO
    r = client.post(f"/reports/generate?hour={HOUR}")
    assert r.status_code == 200
    report = r.json()
    assert "object_key" in report
    assert "link" in report
    object_key = report["object_key"]
    assert object_key == "2026-03-14/1000.csv"

    # 7. GET /reports — el reporte aparece en el listado de MinIO
    r = client.get("/reports")
    assert r.status_code == 200
    names = [rep["name"] for rep in r.json()]
    assert object_key in names

    # 8. GET /reports/{name} — descarga y verifica el contenido
    r = client.get(f"/reports/{object_key}")
    assert r.status_code == 200
    assert "text/csv" in r.headers["content-type"]

    rows = list(csv.DictReader(io.StringIO(r.text)))
    assert len(rows) == 3  # 3 devices

    by_device = {row["device_id"]: row for row in rows}
    assert float(by_device["sensor-01"]["avg_co2"]) == 435.0
    assert float(by_device["sensor-03"]["max_co2"]) == 800.0
    assert int(by_device["sensor-02"]["count"]) == 2

    # 9. Reporte no encontrado devuelve 404
    r = client.get("/reports/nonexistent/report.csv")
    assert r.status_code == 404
