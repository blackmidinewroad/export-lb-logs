import json
import os


def load_movies(filepath: str) -> dict:
    '''Load data from JSON file'''

    if not os.path.exists(filepath):
        return {}
    with open(filepath, 'r') as file:
        return json.load(file)


def save_movies(movies: dict, filepath: str) -> None:
    '''Save data to JSON file'''

    with open(filepath, 'w') as file:
        json.dump(movies, file, indent=2)
