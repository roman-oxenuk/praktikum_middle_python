import sqlite3
from typing import List, Tuple, Dict

import json
from json import JSONDecodeError


class ESLoader:
    def __init__(self, url: str):
        self.url = url

    def load_to_es(self, records: List[dict], index_name: str):
        '''
        Метод для сохранения записей в Elasticsearch.
        :param records: список данных на запись, который должен быть следующего вида:
        [
            {
                "id": "tt123456",
                "genre": ["Action", "Horror"],
                "writers": [
                    {
                        "id": "123456",
                        "name": "Great Divider"
                    },
                    ...
                ],
                "actors": [
                    {
                        "id": "123456",
                        "name": "Poor guy"
                    },
                    ...
                ],
                "actors_names": ["Poor guy", ...],
                "writers_names": [ "Great Divider", ...],
                "imdb_rating": 8.6,
                "title": "A long time ago ...",
                "director": ["Daniel Switch", "Carmen B."],
                "description": "Long and boring description"
            }
        ]
        Если значения нет или оно N/A, то нужно менять на None
        В списках значение N/A надо пропускать
        :param index_name: название индекса, куда будут сохраняться данные
        '''
        # Ваша реализация здесь
        pass


class ETL:
    def __init__(self, conn: sqlite3.Connection, es_loader: ESLoader):
        self.es_loader = es_loader
        self.conn = conn

    def get_writers_data(self, writer: str, writers: str) -> List[Dict]:
        writers_result = []

        try:
            writers_data = json.loads(writers)
            writers_ids = [writer['id']for writer in writers_data]
        except JSONDecodeError:
            writers_ids = [writer]

        writers_ids = [f'"{_id}"' for _id in writers_ids]
        writers_query = ', '.join(writers_ids)

        res = self.conn.execute(
            f'select id, name from writers where id in ({writers_query})'
        )
        for row in res.fetchall():
            if row[1] != 'N/A':
                writers_result.append(
                    {
                        "id": row[0],
                        "name": row[1]
                    }
                )

        return writers_result

    def get_actors_data(self, movie_id: str) -> List[Dict]:
        actors_data = []
        res = self.conn.execute("""SELECT
                actors.id,
                actors.name
            FROM movies
            JOIN movie_actors ON movie_actors.movie_id == movies.id
            JOIN actors ON actors.id == movie_actors.actor_id
            WHERE movies.id = ?;""",
            (movie_id,)
        )
        for row in res.fetchall():
            if row[1] != 'N/A':
                actors_data.append(
                    {
                        "id": row[0],
                        "name": row[1]
                    }
                )
        return actors_data

    def transform_row(self, row: Tuple) -> Dict:
        item = dict()
        item['id'] = row[0]
        item['genre'] = row[1].split(', ')
        item['director'] = row[2] if not 'N/A' else None
        item['title'] = row[4]
        item['description'] = row[5] if not 'N/A' else None
        # row[6] is 'ratings' field
        item['imdb_rating'] = float(row[7]) if not 'N/A' else None

        writers_data = self.get_writers_data(
            row[3], row[8]  # 'writer' and 'writers' fields respectively
        )
        item['writers'] = writers_data
        item['writers_names'] = [writer['name'] for writer in writers_data]

        actors_data = self.get_actors_data(item['id'])
        item['actors'] = actors_data
        item['actors_names'] = [actor['name'] for actor in actors_data]

        return item

    def load(self, index_name: str):
        '''
        Основной метод для нашего ETL.
        Обязательно используйте метод load_to_es, это будет проверяться
        :param index_name: название индекса, в который будут грузиться данные
        '''
        # Ваша реализация ETL здесь
        records = []
        limit = 100
        banch_size = 100
        offset = 0

        while True:
            res = self.conn.execute(
                'select * from movies limit ? offset ?',
                (limit, offset)
            )
            rows = res.fetchall()
            if not rows:
                break
            for row in rows:
                records.append(
                    self.transform_row(row)
                )
            offset += banch_size

        self.es_loader.load_to_es(records, index_name)
