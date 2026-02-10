from app.services.task_status_evaluator import evaluate_task_status


def test_temperature_above_max_is_pending_within_margin():
    sensor = {"temperature": 33.3, "soil_moisture": None}
    thresholds = {"temperature_max": 32.0, "temperature_min": 18.0}
    status, pending_reasons, stop_reasons, _ = evaluate_task_status(
        task_type="fertilization",
        task_title="Fertilization",
        sensor=sensor,
        thresholds=thresholds,
    )

    assert status == "Pending"
    assert stop_reasons == []
    assert any("above max threshold" in reason for reason in pending_reasons)


def test_temperature_above_max_stop_margin_triggers_stop():
    sensor = {"temperature": 42.0, "soil_moisture": None}
    thresholds = {"temperature_max": 32.0, "temperature_min": 18.0}
    status, pending_reasons, stop_reasons, _ = evaluate_task_status(
        task_type="weeding",
        task_title="Weeding",
        sensor=sensor,
        thresholds=thresholds,
    )

    assert status == "Stop"
    assert pending_reasons == []
    assert any("exceeds max threshold" in reason for reason in stop_reasons)


def test_temperature_below_min_is_pending_within_margin():
    sensor = {"temperature": 12.5, "soil_moisture": None}
    thresholds = {"temperature_max": 32.0, "temperature_min": 18.0}
    status, pending_reasons, stop_reasons, _ = evaluate_task_status(
        task_type="weeding",
        task_title="Weeding",
        sensor=sensor,
        thresholds=thresholds,
    )

    assert status == "Pending"
    assert stop_reasons == []
    assert any("below min threshold" in reason for reason in pending_reasons)


def test_temperature_below_min_stop_margin_triggers_stop():
    sensor = {"temperature": 8.0, "soil_moisture": None}
    thresholds = {"temperature_max": 32.0, "temperature_min": 18.0}
    status, pending_reasons, stop_reasons, _ = evaluate_task_status(
        task_type="watering",
        task_title="Watering",
        sensor=sensor,
        thresholds=thresholds,
    )

    assert status == "Stop"
    assert pending_reasons == []
    assert any("below min threshold" in reason for reason in stop_reasons)
