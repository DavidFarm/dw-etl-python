import requests
from src.utils.logger import get_logger

logger = get_logger(__name__)

EXCHANGE_RATE_URL = "https://open.er-api.com/v6/latest/USD"


def get_usd_exchange_rates() -> dict:
    """
    Fetch current USD exchange rates from ExchangeRate-API.
    Returns a dictionary of currency codes to rates.
    """
    logger.info("Fetching USD exchange rates from API...")
    try:
        response = requests.get(EXCHANGE_RATE_URL, timeout=10)
        response.raise_for_status()
        data = response.json()
        rates = data["rates"]
        logger.info(f"Fetched {len(rates)} exchange rates successfully")
        return rates
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch exchange rates: {e}")
        raise