import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta, tzinfo
from typing import Any

UPSERTED = "company-upserted"
STRUCK_OFF = "company-struck-off"

_REGISTRATION_RE = re.compile(r"^SYN-\d{6}$")
_WHITESPACE_RE = re.compile(r"\s+")

_STATES = {
    name.lower(): name
    for name in (
        "Johor", "Kedah", "Kelantan", "Melaka", "Negeri Sembilan", "Pahang", "Perak", "Perlis", "Pulau Pinang",
        "Sabah", "Sarawak", "Selangor", "Terengganu", "Kuala Lumpur", "Labuan", "Putrajaya",
    )
}  # fmt: skip
_STATES |= {
    "penang": "Pulau Pinang",
    "malacca": "Melaka",
    "kl": "Kuala Lumpur",
    "w.p. kuala lumpur": "Kuala Lumpur",
    "wilayah persekutuan kuala lumpur": "Kuala Lumpur",
    "n. sembilan": "Negeri Sembilan",
}


class InvalidEvent(ValueError):
    """The message cannot be applied. `reason` is a short, stable code for the dead-letter table and metrics."""

    def __init__(self, reason: str, detail: str = "") -> None:
        super().__init__(f"{reason}: {detail}" if detail else reason)
        self.reason = reason


@dataclass(frozen=True, slots=True)
class CompanyUpserted:
    registration_number: str
    updated_at: datetime
    company_name: str
    nature_of_business: str | None
    industry: str | None
    state: str | None


@dataclass(frozen=True, slots=True)
class CompanyStruckOff:
    registration_number: str
    updated_at: datetime


Event = CompanyUpserted | CompanyStruckOff


def parse_event(body: bytes, *, source_tz: tzinfo, now: datetime, max_future_skew: timedelta) -> Event:
    try:
        raw = json.loads(body.decode("utf-8"))
    except UnicodeDecodeError, json.JSONDecodeError:
        raise InvalidEvent("malformed_json") from None
    if not isinstance(raw, dict):
        raise InvalidEvent("not_an_object", type(raw).__name__)

    event_type = raw.get("type")
    registration_number = normalise_registration_number(raw.get("registrationNumber"))
    updated_at = parse_timestamp(raw.get("updatedAt"), source_tz=source_tz)
    if updated_at > now + max_future_skew:
        # A far-future version would block every later real update for this company, so refuse it.
        raise InvalidEvent("timestamp_in_future", updated_at.isoformat())

    if event_type == STRUCK_OFF:
        return CompanyStruckOff(registration_number, updated_at)
    if event_type != UPSERTED:
        raise InvalidEvent("unknown_event_type", repr(event_type))

    data = raw.get("data")
    if not isinstance(data, dict):
        raise InvalidEvent("invalid_data", f"expected an object, got {type(data).__name__}")
    company_name = _clean_text(data.get("companyName"), "companyName")
    if company_name is None:
        raise InvalidEvent("missing_company_name")
    return CompanyUpserted(
        registration_number=registration_number,
        updated_at=updated_at,
        company_name=company_name,
        nature_of_business=_clean_text(data.get("natureOfBusiness"), "natureOfBusiness"),
        industry=_clean_text(data.get("industry"), "industry"),
        state=normalise_state(_clean_text(data.get("state"), "state")),
    )


def normalise_registration_number(value: Any) -> str:
    if not isinstance(value, str):
        raise InvalidEvent("invalid_registration_number", repr(value))
    normalised = value.strip().upper()
    if not _REGISTRATION_RE.match(normalised):
        raise InvalidEvent("invalid_registration_number", repr(value))
    return normalised


def parse_timestamp(value: Any, *, source_tz: tzinfo) -> datetime:
    if isinstance(value, bool):  # bool is an int subclass; True is not a timestamp
        raise InvalidEvent("invalid_timestamp", repr(value))
    if isinstance(value, int | float):
        seconds = value / 1000 if abs(value) >= 1e11 else value  # 1e11 s is year 5138, so larger values are ms
        try:
            return datetime.fromtimestamp(seconds, tz=UTC)
        except OverflowError, OSError, ValueError:
            raise InvalidEvent("invalid_timestamp", repr(value)) from None
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.strip())
        except ValueError:
            raise InvalidEvent("invalid_timestamp", repr(value)) from None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=source_tz)
        return parsed.astimezone(UTC)
    raise InvalidEvent("missing_timestamp" if value is None else "invalid_timestamp", repr(value))


def normalise_state(value: str | None) -> str | None:
    if value is None:
        return None
    return _STATES.get(value.lower(), value)


def _clean_text(value: Any, field: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise InvalidEvent("invalid_field_type", f"{field} is {type(value).__name__}")
    cleaned = _WHITESPACE_RE.sub(" ", value).strip()
    return cleaned or None
