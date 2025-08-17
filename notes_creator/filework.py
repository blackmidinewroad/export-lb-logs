import codecs
import json
import os


def load_movies(filepath: str) -> dict:
    """Load data from JSON file."""

    if not os.path.exists(filepath):
        return {}
    with open(filepath, 'r') as file:
        return json.load(file)


def save_movies(movies: dict, filepath: str) -> None:
    """Save data to JSON file."""

    with open(filepath, 'w') as file:
        json.dump(movies, file, indent=2)


def read_note(file_path: str) -> list[str] | None:
    if os.path.exists(file_path):
        with codecs.open(file_path, 'r', 'utf8') as file:
            return file.readlines()
