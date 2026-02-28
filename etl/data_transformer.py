from collections import defaultdict
from typing import List, Dict


class DataTransformer:
    def transform(self, raw_data: List[Dict]) -> List[Dict]:
        """Преобразует необработанные данные из Postgres в формат Elasticsearch."""
        films = defaultdict(lambda: {
            'id': '', 'imdb_rating': 0.0, 'genres': set(), 'title': '',
            'description': '', 'directors_names': [], 'actors_names': [],
            'writers_names': [], 'actors': [], 'writers': [], 'directors': []
        })

        for row in raw_data:
            fid = row['fw_id']
            f = films[fid]
            f['id'] = fid
            f['title'] = row['title']
            f['description'] = row['description']
            f['imdb_rating'] = row['rating']

            if row['genre_name']:
                f['genres'].add(row['genre_name'])

            p_info = {'id': row['person_id'], 'name': row['full_name']}

            if row['role'] == 'director':
                if row['full_name'] not in f['directors_names']:
                    f['directors_names'].append(row['full_name'])
                if p_info not in f['directors']:
                    f['directors'].append(p_info)
            elif row['role'] == 'actor':
                if p_info not in f['actors']:
                    f['actors'].append(p_info)
                    f['actors_names'].append(row['full_name'])
            elif row['role'] == 'writer':
                if p_info not in f['writers']:
                    f['writers'].append(p_info)
                    f['writers_names'].append(row['full_name'])

        for f in films.values():
            f['genres'] = list(f['genres'])

        return list(films.values())
