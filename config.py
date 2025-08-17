import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


class Config:
    RSS_FEED_URL = f'https://letterboxd.com/{os.getenv('LB_USERNAME')}/rss/'

    OBSIDIAN_VAULT_PATH = os.getenv('OBSIDIAN_VAULT_PATH')
    NOT_RATED_FILE = os.getenv('NOT_RATED_FILE')

    EXPORTLB_DATA_DIR = Path.home() / 'ExportLbLogs'
    PROCESSED_LOGS_FILE = EXPORTLB_DATA_DIR / 'processed_movies.json'
    ERROR_LOG_FILE = EXPORTLB_DATA_DIR / 'export_lb.log'

    CHROME_PROFILE_DIR = os.getenv('CHROME_PROFILE_DIR')

    TMDB_ACCESS_TOKEN = os.getenv('TMDB_ACCESS_TOKEN')


def ensure_directories() -> None:
    """Ensure necessary directories exist"""

    Config.EXPORTLB_DATA_DIR.mkdir(parents=True, exist_ok=True)
