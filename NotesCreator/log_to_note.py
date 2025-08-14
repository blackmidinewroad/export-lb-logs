import logging
import os
import sys
from datetime import datetime
from math import floor

import feedparser
from dotenv import load_dotenv
from slugify import slugify

from ..tmdb import api
from .filework import load_movies, read_note, save_movies
from .lb_to_kp import transfer_rating_to_kp

load_dotenv()

LB_USERNAME = os.getenv('LB_USERNAME')
OBSIDIAN_VAULT_PATH = os.getenv('OBSIDIAN_VAULT_PATH')
RSS_FEED_URL = f'https://letterboxd.com/{LB_USERNAME}/rss/'
NOT_RATED_FILE = os.getenv('NOT_RATED_FILE')
PROCESSED_LOGS_FILE = os.path.join(os.path.expanduser('~'), 'processed_movies.json')
ILLEGAL_CHARACTERS = '><:"\\/|?*'


def get_movie_file_path(title: str, year: int, path: str) -> str:
    """Creates path to movie note"""

    for c in ILLEGAL_CHARACTERS:
        title = title.replace(c, '')
    filename = f'{title} - {year}.md' if year else f'{title}.md'
    file_path = os.path.join(path, filename)
    return file_path


def add_to_not_rated(id: str) -> None:
    note = read_note(NOT_RATED_FILE)
    if note is not None:
        note.append(f'\n{id}')
        with open(NOT_RATED_FILE, 'w', encoding='utf8') as file:
            for line in note:
                file.write(line)


def send_to_kp(movie_data: dict) -> None:
    """Rate movie on Kinnopoisk, if couldn't transfer rating to Kinnopoisk - add movie to not rated log file"""

    if movie_data['rating'] and movie_data['year']:
        rated = transfer_rating_to_kp(movie_data)
        if not rated:
            add_to_not_rated(movie_data['id'])


def fetch_data_from_tmdb(tmdb_id: int, tmdb: api.TMDB) -> dict:
    """Fetch more data using TMDB API"""

    tmdb_data = tmdb.fetch_movie_by_id(tmdb_id, append_to_response=['credits'])

    directors = []
    for crew_member in tmdb_data.get('credits', {}).get('crew', []):
        if crew_member['job'] == 'Director':
            directors.append(crew_member.get('name', ''))

    data = {
        'director': directors,
        'genres': [genre['name'] for genre in tmdb_data.get('genres', [])],
        'countries': [ccountry for ccountry in tmdb_data.get('origin_country', [])],
        'original_title': tmdb_data.get('original_title') or '',
        'poster_path': tmdb_data.get('poster_path') or '',
    }

    return data


def fetch_data_from_feed(entry: feedparser.util.FeedParserDict, tmdb: api.TMDB) -> dict:
    movie_year = int(entry.get('letterboxd_filmyear', 0))
    movie_title = entry.get('letterboxd_filmtitle', 'No title')
    movie_id = f'{movie_title} - {movie_year}' if movie_year else f'{movie_title}'
    watched_date = datetime(*entry.published_parsed[:6]).strftime('%d.%m.%Y')
    rating = float(entry.get('letterboxd_memberrating', 0))
    star_rating = ':luc_star:' * floor(rating) + ':luc_star_half:' * (int(rating * 10 % 10) // 5)
    rewatch = entry.get('letterboxd_rewatch', 'No')
    tmdb_id = int(entry.get('tmdb_movieid', 0))

    movie_data = {
        'title': movie_title,
        'year': movie_year,
        'watched_date': watched_date,
        'star_rating': star_rating,
        'rating': rating,
        'id': movie_id,
        'rewatch': rewatch,
        'tmdb_id': tmdb_id,
    }

    movie_data.update(fetch_data_from_tmdb(tmdb_id, tmdb))

    return movie_data


def update_obsidian_note(movie: dict) -> None:
    """Update note if it's a rewatch"""

    file_path = get_movie_file_path(movie['title'], movie['year'], OBSIDIAN_VAULT_PATH)
    note = read_note(file_path)
    if not note:
        return

    star_rating = movie['star_rating'] if movie['star_rating'] else 'none'
    for i in range(len(note)):
        if note[i].startswith('**Rating:**'):
            note[i] = f'**Rating:** {star_rating}\n'

    note.insert(-1, f'> [!NOTE] {movie['watched_date']}\n\n\n')

    if movie['rating']:
        tags = note[-1].split()
        for i in range(len(tags)):
            if tags[i].endswith('rating'):
                tags[i] = f'#{int(movie['rating'] * 2)}-rating'
                break
        else:
            tags.append(f'#{int(movie['rating'] * 2)}-rating')

        if f'#{movie['watched_date'][-4:]}-watched' not in tags:
            tags.append(f'#{movie['watched_date'][-4:]}-watched')

        note[-1] = ' '.join(tags)

    with open(file_path, 'w', encoding='utf8') as file:
        for line in note:
            file.write(line)


def create_obsidian_note(movie: dict) -> None:
    """Create an Obsidian note for a new movie"""

    star_rating = movie['star_rating'] if movie['star_rating'] else 'none'
    file_path = get_movie_file_path(movie['title'], movie['year'], OBSIDIAN_VAULT_PATH)

    with open(file_path, 'w', encoding='utf8') as file:
        if movie['poster_path']:
            file.write(f'![](https://image.tmdb.org/t/p/w185/{movie['poster_path']})\n')
        file.write(f'[URL](https://letterboxd.com/tmdb/{movie['tmdb_id']})\n')

        if movie['director']:
            if len(movie['director']) > 1:
                file.write(f'**Directors:** {', '.join(d for d in movie['director'])}\n')
            else:
                file.write(f'**Director:** {', '.join(d for d in movie['director'])}\n')
        else:
            file.write(f'**Director:** not found\n')

        file.write(f'**Rating:** {star_rating}\n\n')
        file.write(f'---\n\n')
        file.write(f'> [!NOTE] {movie['watched_date']}\n\n\n')

        if movie['director']:
            file.write(f'#{' #'.join(slugify(d) for d in movie['director'])} ')

        if movie['rating']:
            file.write(f'#{int(movie['rating'] * 2)}-rating ')

        if movie['year']:
            file.write(f'#{movie['year'][:len(movie['year']) - 1]}0s ')

        file.write(f'#{movie['watched_date'][-4:]}-watched ')

        if movie['genres']:
            file.write(f'#{' #'.join(slugify(g) for g in movie['genres'])} ')

        if movie['countries']:
            file.write(f'#{' #'.join(slugify(c) for c in movie['countries'])} ')


def main():
    """Main function that checks new entries in RSS feed, updates/creates notes and rates movies on Kinopoisk"""

    logging.basicConfig(level=logging.WARNING, filename='export_lb.log', format='[%(asctime)s] %(levelname)s: %(message)s')

    processed_movies = load_movies(PROCESSED_LOGS_FILE)
    feed = feedparser.parse(RSS_FEED_URL)
    tmdb = api.TMDB()

    for entry in feed.entries[4::-1]:
        if entry.id.find('watch') == -1 and entry.id.find('review') == -1:
            break

        movie_data = fetch_data_from_feed(entry, tmdb)

        if movie_data['watched_date'] in processed_movies.get(movie_data['id'], []):
            continue

        if movie_data['id'] in processed_movies:
            update_obsidian_note(movie_data)
        else:
            create_obsidian_note(movie_data, movie_data['poster_path'])
            send_to_kp(movie_data)

        processed_movies.setdefault(movie_data['id'], []).append(f'{movie_data['watched_date']}')

    save_movies(processed_movies, PROCESSED_LOGS_FILE)


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        logger = logging.getLogger(__name__)
        logger.error('%s', e, exc_info=True)
        sys.exit(1)
