import logging
import httpx


async def fetch_sport(
    client: httpx.AsyncClient, sport: str, url: str, params: dict[str, str]
) -> dict:
    resp = await client.get(url, params=params)
    resp.raise_for_status()
    events_list = resp.json()

    if not events_list:
        logging.warning(f"No {sport} events found in this date range.")
    else:
        logging.info(f"Found {len(events_list)} {sport} events.")
        # Tag each event with the sport

    for e in events_list:
        e["sport_key"] = sport
    return events_list
