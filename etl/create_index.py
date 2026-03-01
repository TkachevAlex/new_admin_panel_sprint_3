import json
import requests
import logging
from .config import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ES_URL = f"http://{settings.elastic_host}:{settings.elastic_port}/movies"dв


def create_index():
    """Чтение схемы из файла и создание индекса в Elasticsearch."""
    try:
        with open('etl/es_schema.txt', 'r') as f:
            schema_data = json.load(f)

        check_resp = requests.get(ES_URL)
        if check_resp.status_code == 200:
            logger.info("Индекс 'movies' уже существует")
            return

        response = requests.put(ES_URL, json=schema_data)

        if response.status_code == 200:
            logger.info("Индекс 'movies' успешно создан")
        else:
            logger.error(f"Ошибка при создании индекса: {response.text}")

    except FileNotFoundError:
        logger.error("Не найден файл es_schema.txt")
    except json.JSONDecodeError:
        logger.error("Ошибка чтения JSON из файла es_schema.txt")
    except requests.exceptions.ConnectionError:
        logger.error("Ошибка связи с ElasticSearch")


if __name__ == "__main__":
    create_index()
