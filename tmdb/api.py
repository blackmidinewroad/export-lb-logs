import asyncio
import logging
import os
from datetime import date, datetime, timedelta
from urllib.parse import urlencode, urljoin

import aiohttp
import requests
from aiolimiter import AsyncLimiter
from ratelimit import limits, sleep_and_retry
from requests.adapters import HTTPAdapter
from tenacity import RetryCallState, retry, retry_if_exception_type, stop_after_attempt, wait_exponential
from urllib3.util import Retry

from tmdb.exceptions import RetryableError

logger = logging.getLogger(__name__)


def retry_error_callback(retry_state: RetryCallState):
    e = retry_state.outcome.exception()
    status = f', status: {e.status}' if e.status else ''

    logger.warning('Failed to fetch data after %s attempts: %s%s.', retry_state.attempt_number, e, status)

    try:
        if isinstance(retry_state.args[1], str):
            path = retry_state.args[1]
    except:
        path = ''

    is_by_id = retry_state.kwargs.get('is_by_id', False)

    if is_by_id and path:
        if e.status and e.status == 404:
            return int(path.split('/')[-1])
        return 0


class BaseTMDB:
    """Base class for TMDB API wrapper."""

    BASE_URL = 'https://api.themoviedb.org/3/'

    def _build_url(self, path: str, params: dict = None) -> str:
        if params is None:
            params = {}

        return f'{urljoin(self.BASE_URL, path)}?{urlencode(params)}'


class TMDB(BaseTMDB):
    """TMDB API wrapper."""

    # calls per 1 second
    calls = 47
    rate_limit = 1

    # Requests retry strategy
    retry = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
    )

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(
            {
                'accept': 'application/json',
                'Authorization': f'Bearer {os.getenv("TMDB_ACCESS_TOKEN")}',
            }
        )
        self.session.mount('https://', HTTPAdapter(max_retries=self.retry))

    @sleep_and_retry
    @limits(calls=calls, period=rate_limit)
    def _fetch_data(self, path: str, params: dict = None) -> dict:
        """Main method to make requests to TMDB API."""

        url = self._build_url(path, params)
        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.warning('Failed to fetch data: %s.', e.__class__.__name__)

            if isinstance(e, requests.exceptions.HTTPError) and e.response.status_code in (401, 403):
                logger.error('Unauthorized or Forbidden: %s, status: %s.', e.__class__.__name__, e.response.status_code)
                raise

    def fetch_genres(self, language: str = 'en') -> list[dict]:
        """Fetch the list of official genres for movies.

        Args:
            language (str, optional): language in ISO 639-1 code (e.g. en, fr, ru). Defaults to 'en'.

        Returns:
            list[dict]: list of genres
        """

        path = 'genre/movie/list'
        params = {'language': language}
        data = self._fetch_data(path, params)

        return data.get('genres', [])

    def _fetch_configuration(self, data_type: str, language: str = None) -> list[dict]:
        """Fetch TMDB configuration - static lists of data they use throughout the database."""

        path = f'configuration/{data_type}'
        params = {'language': language} if language is not None else {}

        return self._fetch_data(path, params)

    def fetch_countries(self, language: str = 'en-US') -> list[dict]:
        """Get the list of countries (ISO 3166-1 tags) used throughout TMDB.

        Args:
            language (str, optional): locale (ISO 639-1-ISO 3166-1) code (e.g. en-US, fr-CA, de-DE). Defaults to 'en-US'.

        Returns:
            list[dict]: list of countries with ISO 3166-1 tags, names and english names.
        """

        data_type = 'countries'

        return self._fetch_configuration(data_type, language=language)

    def fetch_languages(self) -> list[dict]:
        """Get the list of languages (ISO 639-1 tags) used throughout TMDB.

        Returns:
            list[dict]: list of languages with ISO 639-1 tags, names and english names.
        """

        data_type = 'languages'

        return self._fetch_configuration(data_type)

    def _fetch_by_id(self, path: str, language: str = None, append_to_response: list[str] = None) -> dict:
        params = {}
        if language is not None:
            params['language'] = language
        if append_to_response is not None:
            params['append_to_response'] = ','.join(append_to_response)

        return self._fetch_data(path, params)

    def fetch_movie_by_id(self, movie_id: int, language: str = 'en-US', append_to_response: list[str] = None) -> dict:
        """Fetch movie details by ID.

        Args:
            movie_id (int): TMDB ID of a movie.
            language (str, optional): locale (ISO 639-1-ISO 3166-1) code (e.g. en-US, fr-CA, de-DE). Defaults to 'en-US'.
            append_to_response (list[str], optional): list of endpoints within this namespace, 20 items max. Defaults to ''.

        Returns:
            dict: dict with movie details
        """

        path = f'movie/{movie_id}'

        return self._fetch_by_id(path=path, language=language, append_to_response=append_to_response)

    def fetch_person_by_id(self, person_id: int, language: str = 'en-US', append_to_response: list[str] = None) -> dict:
        """Fetch person details by ID.

        Args:
            person_id (int): TMDB ID of a person.
            language (str, optional): locale (ISO 639-1-ISO 3166-1) code (e.g. en-US, fr-CA, de-DE). Defaults to 'en-US'.
            append_to_response (list[str], optional): list of endpoints within this namespace, 20 items max. Defaults to ''.

        Returns:
            dict: dict with person details.
        """

        path = f'person/{person_id}'

        return self._fetch_by_id(path=path, language=language, append_to_response=append_to_response)

    def fetch_company_by_id(self, company_id: int) -> dict:
        """Fetch production company details by ID.

        Args:
            company_id (int): TMDB ID of a production company.

        Returns:
            dict: dict with company details.
        """

        path = f'company/{company_id}'

        return self._fetch_by_id(path=path)

    def fetch_collection_by_id(self, collection_id: int, language: str = 'en-US') -> dict:
        """Fetch collection details by ID.

        Args:
            company_id (int): TMDB ID of a collection.
            language (str, optional): locale (ISO 639-1-ISO 3166-1) code (e.g. en-US, fr-CA, de-DE). Defaults to 'en-US'.

        Returns:
            dict: dict with collection details.
        """

        path = f'collection/{collection_id}'

        return self._fetch_by_id(path=path, language=language)

    def _fetch_pages(self, path: str, first_page: int, last_page: int, language: str, region: str = None) -> list[dict]:
        """Fetch pages of data from endpoints that support pagination."""

        if last_page is None:
            last_page = first_page

        pages = []

        for page in range(first_page, last_page + 1):
            params = {'page': page, 'language': language}
            if region is not None:
                params['region'] = region

            pages.append(self._fetch_data(path, params))

        return pages

    def fetch_popular_movies(self, first_page: int = 1, last_page: int = None, language: str = 'en-US', region: str = None) -> list[dict]:
        """Fetch most popular movies.

        Args:
            first_page (int, optional): first page, max=500. Defaults to 1.
            last_page (int, optional): last page, leave blank if need 1 page, max=500. Defaults to None.
            language (str, optional): locale (ISO 639-1-ISO 3166-1) code (e.g. en-US, fr-CA, de-DE). Defaults to 'en-US'.
            region (str, optional): ISO 3166-1 code (e.g. US, FR, RU). Defaults to None.

        Returns:
            list[dict]: list of pages with movie details.
        """

        path = 'movie/popular'

        return self._fetch_pages(path=path, first_page=first_page, last_page=last_page, language=language, region=region)

    def fetch_top_rated_movies(self, first_page: int = 1, last_page: int = None, language: str = 'en-US', region: str = None) -> list[dict]:
        """Fetch top rated movies.

        Args:
            first_page (int, optional): first page, max=500. Defaults to 1.
            last_page (int, optional): last page, leave blank if need 1 page, max=500. Defaults to None.
            language (str, optional): locale (ISO 639-1-ISO 3166-1) code (e.g. en-US, fr-CA, de-DE). Defaults to 'en-US'.
            region (str, optional): ISO 3166-1 code (e.g. US, FR, RU). Defaults to None.

        Returns:
            list[dict]: list of pages with movie details.
        """

        path = 'movie/top_rated'

        return self._fetch_pages(path=path, first_page=first_page, last_page=last_page, language=language, region=region)

    def fetch_trending_movies(
        self, time_window: str = 'day', first_page: int = 1, last_page: int = None, language: str = 'en-US'
    ) -> list[dict]:
        """Fetch trending movies.

        Args:
            time_window (str, optional): time window, day or week. Defaults to 'day'.
            first_page (int, optional): first page, max=500. Defaults to 1.
            last_page (int, optional): last page, leave blank if need 1 page, max=500. Defaults to None.
            language (str, optional): locale (ISO 639-1-ISO 3166-1) code (e.g. en-US, fr-CA, de-DE). Defaults to 'en-US'.

        Returns:
            list[dict]: list of pages with movie details.
        """

        path = f'trending/movie/{time_window}'

        return self._fetch_pages(path=path, first_page=first_page, last_page=last_page, language=language)

    def fetch_trending_people(
        self, time_window: str = 'day', first_page: int = 1, last_page: int = None, language: str = 'en-US'
    ) -> list[dict]:
        """Fetch trending people.

        Args:
            time_window (str, optional): time window, day or week. Defaults to 'day'.
            first_page (int, optional): first page, max=500. Defaults to 1.
            last_page (int, optional): last page, leave blank if need 1 page, max=500. Defaults to None.
            language (str, optional): locale (ISO 639-1-ISO 3166-1) code (e.g. en-US, fr-CA, de-DE). Defaults to 'en-US'.

        Returns:
            list[dict]: list of pages with people details.
        """

        path = f'trending/person/{time_window}'

        return self._fetch_pages(path=path, first_page=first_page, last_page=last_page, language=language)


class asyncTMDB(BaseTMDB):
    """TMDB API wrapper for async requests."""

    # calls per 1 second
    calls = 47
    rate_limit = 1

    def __init__(self):
        self.headers = {
            'accept': 'application/json',
            'Authorization': f'Bearer {os.getenv("TMDB_ACCESS_TOKEN")}',
        }
        self.limiter = AsyncLimiter(self.calls, self.rate_limit)
        self.session = None

    def run_sync(self, coro):
        """Run async code in a synchronous context."""

        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            return asyncio.run(coro)

        if loop.is_running():
            raise RuntimeError("Can't call sync method from within async event loop")

        return loop.run_until_complete(coro)

    async def _get_session(self):
        if self.session is None or self.session.closed:
            connector = aiohttp.TCPConnector(limit=self.calls)
            timeout = aiohttp.ClientTimeout(total=20)
            self.session = aiohttp.ClientSession(headers=self.headers, connector=connector, timeout=timeout)
        return self.session

    @retry(
        retry=retry_if_exception_type(RetryableError),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry_error_callback=retry_error_callback,
    )
    async def _fetch_data(self, path: str, params: dict = None, is_by_id: bool = False) -> dict | int:
        """Main method to make asynchronous requests to TMDB API."""

        url = self._build_url(path, params)

        async with self.limiter:
            try:
                async with self.session.get(url, timeout=10) as response:
                    response.raise_for_status()
                    return await response.json()

            except aiohttp.ClientResponseError as e:
                if e.status in (401, 403):
                    logger.error('Unauthorized or Forbidden: %s, status: %s.', e.__class__.__name__, e.status)
                    raise
                if e.status in (429, 500, 502, 503, 504):
                    raise RetryableError(e.__class__.__name__, status=e.status)

                if e.status != 404:
                    logger.warning('Failed to fetch data: %s, status: %s.', e.__class__.__name__, e.status)

                if is_by_id:
                    if e.status == 404:
                        return int(path.split('/')[-1])
                    return 0

            except asyncio.TimeoutError as e:
                raise RetryableError(e.__class__.__name__)

            except aiohttp.ClientError as e:
                logger.warning('Failed to fetch data: %s.', e.__class__.__name__)

                if is_by_id:
                    return 0

    async def _batch_fetch(
        self,
        task_details: list[str, dict] | list[str],
        const_params: dict = None,
        is_by_id: bool = False,
    ) -> tuple[list[dict], list[int]]:
        """Fetch one batch of data."""

        results = []
        batch_not_fetched = []

        if const_params is None:
            tasks = [self._fetch_data(path, params, is_by_id=is_by_id) for path, params in task_details]
        else:
            tasks = [self._fetch_data(path, const_params, is_by_id=is_by_id) for path in task_details]

        responses = await asyncio.gather(*tasks)

        for result in responses:
            if isinstance(result, dict):
                results.append(result)
            else:
                batch_not_fetched.append(result)

        return results, batch_not_fetched

    async def _fetch_by_id(
        self,
        paths: list[str],
        language: str = None,
        append_to_response: list[str] = None,
        batch_size: int = 100,
    ) -> tuple[list[dict], list[int]]:
        params = {}
        if language is not None:
            params['language'] = language
        if append_to_response is not None:
            params['append_to_response'] = ','.join(append_to_response)

        all_results = []
        not_fetched = []

        async with await self._get_session():
            for i in range(0, len(paths), batch_size):
                batch = paths[i : i + batch_size]
                results, batch_not_fetched = await self._batch_fetch(task_details=batch, const_params=params, is_by_id=True)
                all_results.extend(results)
                not_fetched.extend(batch_not_fetched)

        # Make sure result contains only data with unique IDs
        unique_results = list({data['id']: data for data in all_results}.values())

        return unique_results, not_fetched

    def fetch_movies_by_id(
        self,
        movie_ids: list[int],
        language: str = 'en-US',
        append_to_response: list[str] = None,
        batch_size: int = 100,
    ) -> tuple[list[dict], list[int]]:
        """Fetch movie details for list of IDs.

        Args:
            movie_ids (list[int]): list of TMDB movie IDs.
            language (str, optional): locale (ISO 639-1-ISO 3166-1) code (e.g. en-US, fr-CA, de-DE). Defaults to 'en-US'.
            append_to_response (list[str], optional): list of endpoints within this namespace,
                will appended to each movie, 20 items max. Defaults to None.
            batch_size (int, optional): number of movies to fetch per batch. Defaults to 100.

        Returns:
            tuple[list[dict], list[int]]: list of movies with details and list of not fetched IDs.
        """

        paths = [f'movie/{movie_id}' for movie_id in movie_ids]

        return self.run_sync(
            self._fetch_by_id(
                paths=paths,
                language=language,
                append_to_response=append_to_response,
                batch_size=batch_size,
            )
        )

    def fetch_people_by_id(
        self,
        person_ids: list[int],
        language: str = 'en-US',
        append_to_response: list[str] = None,
        batch_size: int = 100,
    ) -> tuple[list[dict], list[int]]:
        """Fetch person details for list of IDs.

        Args:
            person_ids (list[int]): list of TMDB person IDs.
            language (str, optional): locale (ISO 639-1-ISO 3166-1) code (e.g. en-US, fr-CA, de-DE). Defaults to 'en-US'.
            append_to_response (list[str], optional): list of endpoints within this namespace,
                will appended to each movie, 20 items max. Defaults to None.
            batch_size (int, optional): number of people to fetch per batch. Defaults to 100.

        Returns:
            tuple[list[dict], list[int]]: list of people with details and list of not fetched IDs.
        """

        paths = [f'person/{person_id}' for person_id in person_ids]

        return self.run_sync(
            self._fetch_by_id(
                paths=paths,
                language=language,
                append_to_response=append_to_response,
                batch_size=batch_size,
            )
        )

    def fetch_companies_by_id(self, company_ids: list[int], batch_size: int = 100) -> tuple[list[dict], list[int]]:
        """Fetch company details for list of IDs.

        Args:
            company_ids (list[int]): list of TMDB company IDs.
            batch_size (int, optional): number of companies to fetch per batch. Defaults to 100.

        Returns:
            tuple[list[dict], list[int]]: list of companies with details and list of not fetched IDs.
        """

        paths = [f'company/{company_id}' for company_id in company_ids]

        return self.run_sync(self._fetch_by_id(paths=paths, batch_size=batch_size))

    def fetch_collections_by_id(
        self, collection_ids: list[int], language: str = 'en-US', batch_size: int = 100
    ) -> tuple[list[dict], list[int]]:
        """Fetch collection details for list of IDs.

        Args:
            collection_ids (list[int]): list of TMDB collection IDs.
            language (str, optional): locale (ISO 639-1-ISO 3166-1) code (e.g. en-US, fr-CA, de-DE). Defaults to 'en-US'.
            batch_size (int, optional): number of collections to fetch per batch. Defaults to 100.

        Returns:
            tuple[list[dict], list[int]]: list of collections with details and list of not fetched IDs.
        """

        paths = [f'collection/{collection_id}' for collection_id in collection_ids]

        return self.run_sync(self._fetch_by_id(paths=paths, language=language, batch_size=batch_size))

    async def _fetch_pages(
        self,
        path: str,
        first_page: int,
        last_page: int,
        change_dates: dict = None,
        language: str = 'en-US',
        region: str = None,
        batch_size: int = 100,
    ) -> list[dict]:
        """Fetch pages of data from endpoints that support pagination."""

        if last_page is None:
            last_page = first_page

        if change_dates is None:
            change_dates = {}

        task_details = []
        for page in range(first_page, last_page + 1):
            detail = {'page': page, 'language': language, 'region': region}
            detail.update(change_dates)
            task_details.append((path, detail))

        all_pages = []

        async with await self._get_session():
            for i in range(0, len(task_details), batch_size):
                batch = task_details[i : i + batch_size]
                results, _ = await self._batch_fetch(task_details=batch)
                all_pages.extend(results)

        return all_pages

    def fetch_popular_movies(
        self,
        first_page: int = 1,
        last_page: int = None,
        language: str = 'en-US',
        region: str = None,
        batch_size: int = 100,
    ) -> list[dict]:
        """Fetch most popular movies.

        Args:
            first_page (int, optional): first page, max=500. Defaults to 1.
            last_page (int, optional): last page, leave blank if need 1 page, max=500. Defaults to None.
            language (str, optional): locale (ISO 639-1-ISO 3166-1) code (e.g. en-US, fr-CA, de-DE). Defaults to 'en-US'.
            region (str, optional): ISO 3166-1 code (e.g. US, FR, RU). Defaults to None.
            batch_size (int, optional): number of pages to fetch per batch. Defaults to 100.

        Returns:
            list[dict]: list of pages with movie details.
        """

        path = 'movie/popular'

        return self.run_sync(
            self._fetch_pages(
                path=path,
                first_page=first_page,
                last_page=last_page,
                language=language,
                region=region,
                batch_size=batch_size,
            )
        )

    def fetch_top_rated_movies(
        self,
        first_page: int = 1,
        last_page: int = None,
        language: str = 'en-US',
        region: str = None,
        batch_size: int = 100,
    ) -> list[dict]:
        """Fetch top rated movies.

        Args:
            first_page (int, optional): first page, max=500. Defaults to 1.
            last_page (int, optional): last page, leave blank if need 1 page, max=500. Defaults to None.
            language (str, optional): locale (ISO 639-1-ISO 3166-1) code (e.g. en-US, fr-CA, de-DE). Defaults to 'en-US'.
            region (str, optional): ISO 3166-1 code (e.g. US, FR, RU). Defaults to None.
            batch_size (int, optional): number of pages to fetch per batch. Defaults to 100.

        Returns:
            list[dict]: list of pages with movie details.
        """

        path = 'movie/top_rated'

        return self.run_sync(
            self._fetch_pages(
                path=path,
                first_page=first_page,
                last_page=last_page,
                language=language,
                region=region,
                batch_size=batch_size,
            )
        )

    def fetch_top_rated_movie_ids(
        self,
        first_page: int = 1,
        last_page: int = None,
        language: str = 'en-US',
        region: str = None,
        batch_size: int = 100,
    ) -> list[int]:
        """Fetch top rated movie IDs.

        Args:
            first_page (int, optional): first page, max=500. Defaults to 1.
            last_page (int, optional): last page, leave blank if need 1 page, max=500. Defaults to None.
            language (str, optional): locale (ISO 639-1-ISO 3166-1) code (e.g. en-US, fr-CA, de-DE). Defaults to 'en-US'.
            region (str, optional): ISO 3166-1 code (e.g. US, FR, RU). Defaults to None.
            batch_size (int, optional): number of pages to fetch per batch. Defaults to 100.

        Returns:
            list[int]: list of IDs of top rated movies.
        """

        path = 'movie/top_rated'

        pages = self.run_sync(
            self._fetch_pages(
                path=path,
                first_page=first_page,
                last_page=last_page,
                language=language,
                region=region,
                batch_size=batch_size,
            )
        )

        return [movie['id'] for page in pages for movie in page['results'] if not movie['adult']]

    def fetch_trending_movies(
        self,
        time_window: str = 'day',
        first_page: int = 1,
        last_page: int = None,
        language: str = 'en-US',
        batch_size: int = 100,
    ) -> list[dict]:
        """Fetch trending movies.

        Args:
            time_window (str, optional): time window, day or week. Defaults to 'day'.
            first_page (int, optional): first page, max=500. Defaults to 1.
            last_page (int, optional): last page, leave blank if need 1 page, max=500. Defaults to None.
            language (str, optional): locale (ISO 639-1-ISO 3166-1) code (e.g. en-US, fr-CA, de-DE). Defaults to 'en-US'.
            batch_size (int, optional): number of pages to fetch per batch. Defaults to 100.

        Returns:
            list[dict]: list of pages with movie details.
        """

        path = f'trending/movie/{time_window}'

        return self.run_sync(
            self._fetch_pages(
                path=path,
                first_page=first_page,
                last_page=last_page,
                language=language,
                batch_size=batch_size,
            )
        )

    def fetch_trending_people(
        self,
        time_window: str = 'day',
        first_page: int = 1,
        last_page: int = None,
        language: str = 'en-US',
        batch_size: int = 100,
    ) -> list[dict]:
        """Fetch trending people.

        Args:
            time_window (str, optional): time window, day or week. Defaults to 'day'.
            first_page (int, optional): first page, max=500. Defaults to 1.
            last_page (int, optional): last page, leave blank if need 1 page, max=500. Defaults to None.
            language (str, optional): locale (ISO 639-1-ISO 3166-1) code (e.g. en-US, fr-CA, de-DE). Defaults to 'en-US'.
            batch_size (int, optional): number of pages to fetch per batch. Defaults to 100.

        Returns:
            list[dict]: list of pages with people details.
        """

        path = f'trending/person/{time_window}'

        return self.run_sync(
            self._fetch_pages(
                path=path,
                first_page=first_page,
                last_page=last_page,
                language=language,
                batch_size=batch_size,
            )
        )

    def fetch_changed_ids(self, ids_type: str, days: int = 1, batch_size: int = 100) -> tuple[set[int], date]:
        """Fetch changed movies/people in the past N days.

        Args:
            ids_type (str): 'movie' or 'person'.
            days (int, optional): for how many last days to fetch changes. Defaults to 1.
            batch_size (int, optional): number of pages to fetch per batch. Defaults to 100.

        Raises:
            ValueError: if id_type is not 'movie' or 'person'.

        Returns:
            tuple[set[int], date]: set of IDs and earliest date of changes.
        """

        if ids_type not in ('movie', 'person'):
            raise ValueError("Invalid ids_type, must be 'movie' or 'person'.")

        path = f'{ids_type}/changes'

        cur_date = datetime.now()
        ids = set()

        for _ in range(1, days + 1):
            cur_date_str = str(cur_date.date())
            params = {'start_date': cur_date_str, 'end_date': cur_date_str}

            first_page_data = self.run_sync(self._fetch_pages(path=path, first_page=1, last_page=1, change_dates=params))[0]

            if first_page_data is None or (total_pages := first_page_data.get('total_pages')) is None:
                logger.warning("Couldn't fetch changes for %s.", cur_date_str)
                continue

            data = self.run_sync(
                self._fetch_pages(
                    path=path,
                    first_page=1,
                    last_page=min(total_pages, 500),  # Max. page is 500
                    change_dates=params,
                    batch_size=batch_size,
                )
            )

            if ids_type == 'movie':
                ids.update(movie['id'] for page in data for movie in page['results'] if not movie['adult'])
            elif ids_type == 'person':
                ids.update(person['id'] for page in data for person in page['results'])

            cur_date -= timedelta(days=1)

        earliest_date = (cur_date + timedelta(days=1)).date()

        return ids, earliest_date
