from typing import Any, Dict, List, Optional, Tuple
import logging

logger = logging.getLogger(__name__)
STOP_MARGIN = 10.0


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _resolve_temperature_bounds(
    thresholds: Dict[str, float],
) -> Tuple[Optional[float], Optional[float]]:
    base_max = thresholds.get("temperature_max")
    if base_max is None:
        base_max = thresholds.get("temperature_pending_max")
    if base_max is None:
        base_max = thresholds.get("temperature_stop_max")

    base_min = thresholds.get("temperature_min")
    if base_min is None:
        base_min = thresholds.get("temperature_pending_min")
    if base_min is None:
        base_min = thresholds.get("temperature_stop_min")

    return base_min, base_max


def evaluate_task_status(
    task_type: str,
    task_title: str,
    sensor: Dict[str, Any],
    thresholds: Dict[str, float],
    stop_margin: float = STOP_MARGIN,
) -> Tuple[str, List[str], List[str], List[str]]:
    """
    Single source-of-truth status evaluator.

    Returns:
      status, pending_reasons, stop_reasons, debug_notes
    """
    task_type_normalized = (task_type or "").strip().lower()
    debug_notes: List[str] = []
    pending_reasons: List[str] = []
    stop_reasons: List[str] = []

    soil_moisture_val = _safe_float(sensor.get("soil_moisture"))
    temperature_val = _safe_float(sensor.get("temperature"))

    moisture_max = thresholds.get("soil_moisture_max")
    moisture_field_max = thresholds.get("soil_moisture_field_max")

    temp_min, temp_max = _resolve_temperature_bounds(thresholds)

    if soil_moisture_val is None:
        debug_notes.append("Soil moisture value missing; moisture rules skipped")
    if temperature_val is None:
        debug_notes.append("Temperature value missing; temperature rules skipped")
    if moisture_max is None:
        debug_notes.append("soil_moisture_max threshold missing; moisture rules skipped")
    if moisture_field_max is None:
        debug_notes.append("soil_moisture_field_max threshold missing; field moisture rules skipped")
    if temp_max is None:
        debug_notes.append("temperature_max threshold missing; max temp rule skipped")
    if temp_min is None:
        debug_notes.append("temperature_min threshold missing; min temp rule skipped")

    if task_type_normalized in ("watering", "irrigation"):
        if soil_moisture_val is not None and moisture_max is not None:
            if soil_moisture_val > moisture_max:
                delta = soil_moisture_val - moisture_max
                if delta >= stop_margin:
                    stop_reasons.append(
                        "Soil moisture "
                        f"{soil_moisture_val:.1f}% exceeds max threshold "
                        f"({moisture_max:.1f}%) by {delta:.1f}% (>= {stop_margin:.1f})"
                    )
                else:
                    pending_reasons.append(
                        f"Soil moisture {soil_moisture_val:.1f}% is above max threshold "
                        f"({moisture_max:.1f}%) but not extreme (margin < {stop_margin:.1f})"
                    )

    if task_type_normalized in ("weeding", "land-prep", "fertilization"):
        if soil_moisture_val is not None and moisture_field_max is not None:
            if soil_moisture_val > moisture_field_max:
                delta = soil_moisture_val - moisture_field_max
                if delta >= stop_margin:
                    stop_reasons.append(
                        "Soil moisture "
                        f"{soil_moisture_val:.1f}% exceeds max threshold "
                        f"({moisture_field_max:.1f}%) by {delta:.1f}% (>= {stop_margin:.1f})"
                    )
                else:
                    pending_reasons.append(
                        "Soil moisture "
                        f"{soil_moisture_val:.1f}% is above max threshold "
                        f"({moisture_field_max:.1f}%) but not extreme (margin < {stop_margin:.1f})"
                    )

    if temperature_val is not None:
        if temp_max is not None:
            if temperature_val >= temp_max + stop_margin:
                delta = temperature_val - temp_max
                stop_reasons.append(
                    f"Temperature {temperature_val:.1f}C exceeds max threshold "
                    f"({temp_max:.1f}C) by {delta:.1f}C (>= {stop_margin:.1f}C)"
                )
            elif temperature_val > temp_max:
                delta = temperature_val - temp_max
                pending_reasons.append(
                    f"Temperature {temperature_val:.1f}C is above max threshold "
                    f"({temp_max:.1f}C) by {delta:.1f}C but not extreme (margin < {stop_margin:.1f}C)"
                )

        if temp_min is not None:
            if temperature_val <= temp_min - stop_margin:
                delta = temp_min - temperature_val
                stop_reasons.append(
                    f"Temperature {temperature_val:.1f}C below min threshold "
                    f"({temp_min:.1f}C) by {delta:.1f}C (>= {stop_margin:.1f}C)"
                )
            elif temperature_val < temp_min:
                delta = temp_min - temperature_val
                pending_reasons.append(
                    f"Temperature {temperature_val:.1f}C is below min threshold "
                    f"({temp_min:.1f}C) by {delta:.1f}C but not extreme (margin < {stop_margin:.1f}C)"
                )

    status = "Proceed"
    if stop_reasons:
        status = "Stop"
    elif pending_reasons:
        status = "Pending"

    logger.info(
        "Task evaluation: type=%s temp=%s soil=%s status=%s temp_min=%s temp_max=%s stop_margin=%s",
        task_type_normalized,
        temperature_val,
        soil_moisture_val,
        status,
        temp_min,
        temp_max,
        stop_margin,
    )

    return status, pending_reasons, stop_reasons, debug_notes
