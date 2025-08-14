import codecs
import json
import logging
import os
import re
import sys
import time
from datetime import datetime
from math import floor

import feedparser
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from filework import load_movies, save_movies
from lb_to_kp import transfer_rating_to_kp
from slugify import slugify

load_dotenv()

LB_USERNAME = os.getenv('LB_USERNAME')
OBSIDIAN_VAULT_PATH = os.getenv('OBSIDIAN_VAULT_PATH')
RSS_FEED_URL = f'https://letterboxd.com/{LB_USERNAME}/rss/'
POSTER_FOLDER_PATH = os.getenv('POSTER_FOLDER_PATH')
NOT_RATED_FILE = os.getenv('NOT_RATED_FILE')
PROCESSED_LOGS_FILE = os.path.join(os.path.expanduser('~'), 'processed_movies.json')
ILLEGAL_CHARACTERS = '><:"\\/|?*'


def get_movie_file_path(title, year, path):
    for c in ILLEGAL_CHARACTERS:
        title = title.replace(c, '')
    filename = f'{title} - {year}.md' if year else f'{title}.md'
    file_path = os.path.join(path, filename)
    return file_path


# Remove username from URL and number at the end if it's there
def transform_url(url):
    pattern = r'/\d{1}/$'
    url = url[: url.find(LB_USERNAME)] + url[url.find(LB_USERNAME) + len(LB_USERNAME) + 1 :]
    url = re.sub(pattern, '/', url)
    return url


# Download and save the poster image
def download_poster(image_url, title):
    poster_path = os.path.join(POSTER_FOLDER_PATH, f'{title}.jpg')
    response = requests.get(image_url)
    if response.status_code == 200:
        with open(poster_path, 'wb') as file:
            file.write(response.content)
        return poster_path


def read_note(file_path):
    if os.path.exists(file_path):
        with codecs.open(file_path, 'r', 'utf8') as file:
            return file.readlines()


def open_movie_page(letterboxd_url):
    retries = 0
    while retries < 3:
        response = requests.get(letterboxd_url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            return soup
        elif response.status_code == 429:
            retry_after = int(response.headers.get('Retry-After', 60)) + 1
            time.sleep(retry_after)
            retries += 1
        else:
            break


def add_to_not_rated(id):
    note = read_note(NOT_RATED_FILE)
    note.append(f'\n{id}')
    with open(NOT_RATED_FILE, 'w', encoding='utf8') as file:
        for line in note:
            file.write(line)


def send_to_kp(movie_data):
    if movie_data['rating'] and movie_data['year']:
        rated = transfer_rating_to_kp(movie_data)
        if not rated:
            add_to_not_rated(movie_data['id'])


# Find russian title in alternative titles
def get_russian_title(alt_tag):
    alt_titles = alt_tag.get_text().split(', ')
    for title in alt_titles:
        title = title.strip('\n\t')
        for c in title:
            if c.isalpha():
                try:
                    if not (1072 <= ord(c.lower()) <= 1103):
                        break
                except Exception:
                    pass
        else:
            return title


# Fetch more data using movie page
def fetch_data_from_movie_page(letterboxd_url):
    data = {'director': [], 'genres': [], 'countries': [], 'other_titles': [], 'poster_url': None}
    soup = open_movie_page(letterboxd_url)
    if not soup:
        return data

    script_tag = soup.find('script', type='application/ld+json')
    if not script_tag:
        return data

    json_text = re.sub(r'/\*.*?\*/', '', script_tag.string, flags=re.DOTALL).strip()
    json_data = json.loads(json_text)

    if 'image' in json_data:
        data['poster_url'] = json_data['image']

    if 'director' in json_data:
        for director in json_data['director']:
            data['director'].append(director['name'])

    if 'genre' in json_data:
        data['genres'] = json_data['genre']

    if 'countryOfOrigin' in json_data:
        for country in json_data['countryOfOrigin']:
            data['countries'].append(country['name'])

    original_tag = soup.find('h2', {'class': 'originalname'})
    if original_tag:
        data['other_titles'].append(original_tag.get_text())

    details_soup = open_movie_page(f'{letterboxd_url}details/')
    if details_soup:
        alt_tag = details_soup.find('div', {'class': 'text-indentedlist'})
        if alt_tag:
            russian_title = get_russian_title(alt_tag)
            if russian_title:
                data['other_titles'].append(russian_title)

    return data


# Fetching data from RSS feed
def fetch_data_from_feed(entry):
    movie_year = entry.get('letterboxd_filmyear', 0)
    movie_title = entry.get('letterboxd_filmtitle', 'No title')
    movie_id = f'{movie_title} - {movie_year}' if movie_year else f'{movie_title}'
    watched_date = datetime(*entry.published_parsed[:6]).strftime('%d.%m.%Y')
    rating = float(entry.get('letterboxd_memberrating', 0))
    star_rating = ':luc_star:' * floor(rating) + ':luc_star_half:' * (int(rating * 10 % 10) // 5)
    movie_url = transform_url(entry.link)
    rewatch = entry.get('letterboxd_rewatch', 'No')

    movie_data = {
        'title': movie_title,
        'year': movie_year,
        'watched_date': watched_date,
        'star_rating': star_rating,
        'rating': rating,
        'movie_url': movie_url,
        'id': movie_id,
        'rewatch': rewatch,
    }

    movie_data.update(fetch_data_from_movie_page(movie_url))

    poster_path = download_poster(movie_data['poster_url'], slugify(movie_title)) if movie_data['poster_url'] else None
    del movie_data['poster_url']
    movie_data.setdefault('poster_path', poster_path)

    return movie_data


# Update note if it's a rewatch
def update_obsidian_note(movie):
    file_path = get_movie_file_path(movie['title'], movie['year'], OBSIDIAN_VAULT_PATH)
    note = read_note(file_path)
    if not note:
        return None

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


# Create an Obsidian note for a new movie
def create_obsidian_note(movie, poster_path=None):
    star_rating = movie['star_rating'] if movie['star_rating'] else 'none'
    file_path = get_movie_file_path(movie['title'], movie['year'], OBSIDIAN_VAULT_PATH)

    with open(file_path, 'w', encoding='utf8') as file:
        if poster_path:
            relative_poster_path = os.path.relpath(poster_path, OBSIDIAN_VAULT_PATH)
            file.write(f'![[{relative_poster_path[relative_poster_path.find('\\') + 1:]}]]\n')
        file.write(f'[URL]({movie['movie_url']})\n')

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


# Main function that checks new entries in RSS feed
def main():
    logging.basicConfig(level=logging.WARNING, filename='export_lb.log', format='[%(asctime)s] %(levelname)s: %(message)s')

    processed_movies = load_movies(PROCESSED_LOGS_FILE)
    feed = feedparser.parse(RSS_FEED_URL)

    for entry in feed.entries[4::-1]:
        if entry.id.find('watch') == -1 and entry.id.find('review') == -1:
            break

        movie_data = fetch_data_from_feed(entry)
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
