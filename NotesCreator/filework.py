import json
import os
import traceback


# Log all errors to file
def log_error_to_file():
    with open('D:/Python/Projects/Letterboxd/dist/error_log.txt', 'a') as file:
        file.write(traceback.format_exc())


# Load data from JSON file
def load_movies(filepath):
    if not os.path.exists(filepath):
        return {}
    with open(filepath, 'r') as file:
        try:
            return json.load(file)
        except Exception:
            log_error_to_file()


# Save data to JSON file
def save_movies(movies, filepath):
    with open(filepath, 'w') as file:
        json.dump(movies, file, indent=2)
