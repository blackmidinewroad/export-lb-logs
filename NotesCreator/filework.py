import json
import os
import traceback
import sys
from dotenv import load_dotenv

load_dotenv()


def log_error_to_file() -> None:
    with open(os.getenv('ERROR_LOG_FILE'), 'a') as file:
        file.write(traceback.format_exc())


def load_movies(filepath: str) -> dict:
    '''Load data from JSON file'''

    if not os.path.exists(filepath):
        return {}
    with open(filepath, 'r') as file:
        try:
            return json.load(file)
        except Exception:
            log_error_to_file()
            sys.exit()


def save_movies(movies: dict, filepath: str) -> None:
    '''Save data to JSON file'''

    with open(filepath, 'w') as file:
        json.dump(movies, file, indent=2)
