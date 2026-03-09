import requests
import pandas as pd
from datetime import datetime
from io import StringIO
from app.models import NewsEvent

# ForexFactory weekly calendar export
# (same file downloaded via website Export button)
FF_EXPORT_URL = "https://nfs.faireconomy.media/ff_calendar_thisweek.csv"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (TradingHub/1.0)"
}

IMPACT_MAP = {
    "Low": "low",
    "Medium": "medium",
    "High": "high"
}


def download_calendar():
    """
    Downloads ForexFactory calendar automatically.
    """
    r = requests.get(FF_EXPORT_URL, headers=HEADERS, timeout=30)
    r.raise_for_status()

    return pd.read_csv(StringIO(r.text))


def parse_events(df):
    events = []

    for _, row in df.iterrows():

        currency = str(row["Currency"]).upper()
        impact = IMPACT_MAP.get(str(row["Impact"]).strip(), "low")

        ts = pd.to_datetime(
            f"{row['Date']} {row['Time']}",
            utc=True,
            errors="coerce"
        )

        if pd.isna(ts):
            continue

        events.append(
            NewsEvent(
                timestamp=ts.to_pydatetime(),
                currency=currency,
                impact=impact,
                title=row["Event"],
                forecast=row.get("Forecast"),
                previous=row.get("Previous"),
                actual=row.get("Actual"),
                source="forexfactory_auto"
            )
        )

    return events


def get_usd_high_events():
    df = download_calendar()
    events = parse_events(df)

    return [
        e for e in events
        if e.currency == "USD" and e.impact == "high"
    ]
