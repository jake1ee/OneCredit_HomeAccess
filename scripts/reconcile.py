import argparse
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import psycopg

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.config import get_settings
from app.events import CompanyStruckOff, CompanyUpserted, InvalidEvent, parse_event
from app.runtime import run


def expected_state(lines: list[str], source_tz) -> tuple[dict, int]:
    latest: dict[str, CompanyUpserted | CompanyStruckOff] = {}
    invalid = 0
    for line in lines:
        try:
            event = parse_event(
                line.encode(), source_tz=source_tz, now=datetime.now(UTC), max_future_skew=timedelta(days=1)
            )
        except InvalidEvent:
            invalid += 1
            continue
        current = latest.get(event.registration_number)
        if current is None or event.updated_at > current.updated_at:
            latest[event.registration_number] = event
    return latest, invalid


async def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--file", type=Path, default=Path("data/feed.jsonl"))
    args = parser.parse_args()
    settings = get_settings()

    lines = args.file.read_text(encoding="utf-8").splitlines()
    expected, invalid = expected_state(lines, ZoneInfo(settings.source_timezone))

    async with await psycopg.AsyncConnection.connect(settings.database_url) as conn:
        cur = await conn.execute(
            "SELECT registration_number, company_name, nature_of_business, industry, state, is_struck_off,"
            " source_updated_at, embedding IS NOT NULL FROM companies"
        )
        actual = {row[0]: row for row in await cur.fetchall()}
        cur = await conn.execute("SELECT count(*) FROM dead_letters")
        (dead_letters,) = await cur.fetchone()

    problems = []
    for reg, event in expected.items():
        row = actual.get(reg)
        if row is None:
            problems.append(f"{reg}: missing from the database")
            continue
        _, name, nob, industry, state, struck_off, updated_at, has_embedding = row
        if updated_at != event.updated_at:
            problems.append(f"{reg}: version {updated_at} but latest event is {event.updated_at}")
        if isinstance(event, CompanyStruckOff):
            if not struck_off:
                problems.append(f"{reg}: should be struck off")
            continue
        if struck_off:
            problems.append(f"{reg}: struck off but latest event is an upsert")
        if (name, nob, industry, state) != (event.company_name, event.nature_of_business, event.industry, event.state):
            problems.append(f"{reg}: fields differ from the latest event")
        if not has_embedding:
            problems.append(f"{reg}: no embedding")
    problems += [f"{reg}: in the database but not in the feed" for reg in actual.keys() - expected.keys()]

    struck = sum(isinstance(e, CompanyStruckOff) for e in expected.values())
    print(f"feed lines:            {len(lines)}")
    print(f"unusable lines:        {invalid}   (dead_letters table holds {dead_letters} distinct messages)")
    print(f"companies expected:    {len(expected)}   ({struck} struck off)")
    print(f"companies in database: {len(actual)}")
    print(f"mismatches:            {len(problems)}")
    for problem in problems[:20]:
        print("  -", problem)
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(run(main()))
