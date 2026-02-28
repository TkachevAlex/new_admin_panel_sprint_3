import time
import logging
import psycopg2
from .config import settings
from .state import State, JsonFileStorage
from .postgres_extractor import PostgresExtractor
from .data_transformer import DataTransformer
from .elasticsearch_loader import ElasticsearchLoader
from .models import Movie

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def etl_process():
    """Запускает основной процесс ETL для извлечения, преобразования и загрузки данных."""
    logger.info("Начало ETL процесса...")

    state = State(JsonFileStorage('state.json'))

    dsn = {
        'dbname': settings.postgres_db,
        'user': settings.postgres_user,
        'password': settings.postgres_password,
        'host': settings.db_host,
        'port': settings.db_port
    }

    try:
        with psycopg2.connect(**dsn) as pg_conn:
            logger.info("Соединение с PostgreSQL установлено")

            extractor = PostgresExtractor(pg_conn)
            transformer = DataTransformer()
            loader = ElasticsearchLoader(f"http://{settings.elastic_host}:"
                                         f"{settings.elastic_port}")

            while True:
                all_film_ids = set()
                any_changes = False

                for table in ['person', 'genre', 'film_work']:
                    state_key = f'{table}_modified'
                    last_mod = state.get_state(state_key) or '1900-01-01'

                    modified = extractor.get_modified_ids(table, last_mod)

                    if modified:
                        any_changes = True
                        m_ids = [m['id'] for m in modified]
                        logger.info(f"Обнаружено изменений в {table}: "
                                    f"{len(m_ids)}")

                        if table == 'person':
                            all_film_ids.update(
                                extractor.get_film_ids_by_persons(m_ids)
                            )
                        elif table == 'genre':
                            all_film_ids.update(
                                extractor.get_film_ids_by_genres(m_ids)
                            )
                        else:
                            all_film_ids.update(m_ids)

                        state.set_state(
                            state_key,
                            str(modified[-1]['modified'])
                        )

                if all_film_ids:
                    logger.info(f"Обработка {len(all_film_ids)} фильмов")

                    raw_data = extractor.get_full_films_data(list(all_film_ids))

                    movies_data = transformer.transform(raw_data)

                    movies_objects = [Movie(**m) for m in movies_data]

                    loader.load(movies_objects)

                    logger.info("Синхронизация с Elasticsearch завершена")

                elif not any_changes:
                    logger.info("Нет новых изменений. Ожидание...")

                time.sleep(5)

    except Exception as e:
        logger.error(f"Критическая ошибка: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    etl_process()
