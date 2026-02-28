import json
import requests
from typing import List
from .models import Movie
from .backoff import backoff


class ElasticsearchLoader:
    def __init__(self, url: str):
        self.url = f"{url}/_bulk"

    @backoff()
    def load(self, movies: List[Movie]):
        """Загружает список фильмов в Elasticsearch через bulk-запрос."""
        if not movies:
            return

        bulk_query = []
        for movie in movies:
            index_row = {"index": {"_index": "movies", "_id": str(movie.id)}}
            bulk_query.append(json.dumps(index_row))
            bulk_query.append(movie.model_dump_json())

        data = "\n".join(bulk_query) + "\n"

        response = requests.post(
            self.url,
            data=data,
            headers={'Content-Type': 'application/x-ndjson'}
        )
        response.raise_for_status()
