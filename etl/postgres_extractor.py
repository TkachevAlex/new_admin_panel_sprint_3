from typing import List, Dict, Any


class PostgresExtractor:
    def __init__(self, connection):
        self.connection = connection

    def get_modified_ids(self,
                         table: str,
                         last_modified: str,
                         limit: int = 100) -> List[Dict[str, Any]]:
        """Извлекает список id измененных записей из указанной таблицы."""
        query = (f"SELECT id, modified FROM content.{table} "
                 f"WHERE modified > %s ORDER BY modified LIMIT %s;")
        with self.connection.cursor() as cur:
            cur.execute(query, (last_modified, limit))
            columns = [col[0] for col in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]

    def get_film_ids_by_persons(self, person_ids: List[str]) -> List[str]:
        """Возвращает список id фильмов для указанных id персон."""
        if not person_ids:
            return []
        query = """
            SELECT
                film_work_id
            FROM content.person_film_work
            WHERE person_id IN %s;
        """
        with self.connection.cursor() as cur:
            cur.execute(query, (tuple(person_ids),))
            return [row[0] for row in cur.fetchall()]

    def get_film_ids_by_genres(self, genre_ids: List[str]) -> List[str]:
        """Возвращает список id фильмов для указанных id жанров."""
        if not genre_ids:
            return []
        query = """
            SELECT
                film_work_id
            FROM content.genre_film_work
            WHERE genre_id IN %s;
        """
        with self.connection.cursor() as cur:
            cur.execute(query, (tuple(genre_ids),))
            return [row[0] for row in cur.fetchall()]

    def get_full_films_data(self, film_ids: List[str]) -> List[Dict[str, Any]]:
        """Извлекает полные данные о фильмах по их id."""
        if not film_ids:
            return []
        query = """
            SELECT
                fw.id as fw_id, fw.title, fw.description, fw.rating, fw.type,
                fw.modified, pfw.role, p.id as person_id,
                p.full_name, g.name as genre_name
            FROM content.film_work fw
            LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
            LEFT JOIN content.person p ON p.id = pfw.person_id
            LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
            LEFT JOIN content.genre g ON g.id = gfw.genre_id
            WHERE fw.id IN %s;
        """
        with self.connection.cursor() as cur:
            cur.execute(query, (tuple(film_ids),))
            columns = [col[0] for col in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]
