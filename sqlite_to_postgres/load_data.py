"""Script for transferring data from SQLite to PostgreSQL."""
import os
import logging
import sqlite3
from contextlib import closing
from dataclasses import astuple, fields
from typing import Any, Generator

import psycopg
from psycopg.rows import dict_row
from dotenv import load_dotenv

from models import FilmWork, Genre, GenreFilmWork, Person, PersonFilmWork


load_dotenv()

# Get connection parameters
sqlite_db_path = os.getenv('SQLITE_DB')
if sqlite_db_path is None:
    raise ValueError("SQLITE_DB environment variable is not set")

dbname = os.getenv('POSTGRES_DB')
user = os.getenv('POSTGRES_USER')
password = os.getenv('POSTGRES_PASSWORD')
host = os.getenv('POSTGRES_HOST')
port = os.getenv('POSTGRES_PORT')

tables_maps = {
    'film_work': FilmWork,
    'genre': Genre,
    'person': Person,
    'genre_film_work': GenreFilmWork,
    'person_film_work': PersonFilmWork}

BATCH_SIZE = 100
OPTIONS = '-c search_path=content'

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

ch.setFormatter(formatter)
logger.addHandler(ch)


def extract_data(
        sqlite_cursor: sqlite3.Cursor,
        table_name: str
) -> Generator[list[sqlite3.Row], None, None]:
    """
    Extract data from SQLite table in batches.

    Args:
        sqlite_cursor: SQLite cursor for database operations
        table_name: Name of the table to extract data from

    Yields:
        Generator[list[sqlite3.Row], None, None]: Batch of rows from the table

    Raises:
        Exception: If there's an error extracting data from the table
    """
    try:
        sqlite_cursor.execute(f'SELECT * FROM {table_name}')
        while results := sqlite_cursor.fetchmany(BATCH_SIZE):
            yield results
    except Exception as e:
        logger.error("Error extracting data from %s: %s", table_name, e)
        raise


def transform_data(
        sqlite_cursor: sqlite3.Cursor,
        data_table: str,
        data_class_model: FilmWork
) -> Generator[list[FilmWork], None, None]:
    """
    Transform film work data from SQLite to FilmWork objects.

    Args:
        sqlite_cursor: SQLite cursor for database operations

    Yields:
        Generator[list[Genre], None, None]: Batch of FilmWork objects
    """
    for batch in extract_data(sqlite_cursor, data_table):
        yield [data_class_model(**dict(item)) for item in batch]


def load_data(
        sqlite_cursor: sqlite3.Cursor,
        pg_cursor: psycopg.Cursor[tuple[Any, ...]],
        tables_maps: dict[str, type]) -> None:
    for data_table, data_model in tables_maps.items():

        column_names = [f.name for f in fields(data_model)]
        column_names_str = ','.join(column_names)
        if 'created_at' in column_names:
            column_names_str = column_names_str.replace(
                'created_at', 'created')
        if 'updated_at' in column_names:
            column_names_str = column_names_str.replace(
                'updated_at', 'modified')
        col_count = ', '.join(['%s'] * len(column_names))

        try:
            for batch in transform_data(sqlite_cursor, data_table, data_model):
                pg_cursor.executemany(
                        f'INSERT INTO {data_table} ({column_names_str}) '
                        f'VALUES ({col_count}) ON CONFLICT (id) DO NOTHING',
                        [astuple(item) for item in batch])
                logger.info(f'Loaded {len(batch)} {data_table} records')
        except Exception as e:
            logger.error(f'Error loading {data_table} data: {e}')
            raise


def test_transfer_film_work_table(
        sqlite_cursor: sqlite3.Cursor,
        pg_cursor: psycopg.Cursor[dict[str, Any]]
) -> None:
    """
    Test the transfer of film work data from SQLite to PostgreSQL.

    This function compares the data in the SQLite film_work table with
    the data in the PostgreSQL film_work table to ensure they match.

    Args:
        sqlite_cursor: SQLite cursor for reading data
        pg_cursor: PostgreSQL cursor for reading data
    """
    sqlite_cursor.execute('SELECT * FROM film_work ORDER BY id')

    while batch := sqlite_cursor.fetchmany(BATCH_SIZE):
        original_film_work_batch = [
            FilmWork(**dict(film_work))
            for film_work in batch
        ]
        ids = [film_work.id for film_work in original_film_work_batch]
        pg_cursor.execute(
            'SELECT id, '
            'title, '
            'description, '
            'creation_date ,'
            'rating ,'
            'type, '
            'created::text as created_at, '
            'modified::text as updated_at '
            'FROM film_work '
            'WHERE id = ANY(%s) '
            'ORDER BY id', [ids]
        )

        transferred_film_work_batch = [
            FilmWork(**dict(film_work))
            for film_work in pg_cursor.fetchall()
        ]

        assert (
            len(original_film_work_batch) == len(transferred_film_work_batch)
        )
        assert original_film_work_batch == transferred_film_work_batch


def test_transfer_genre_table(
        sqlite_cursor: sqlite3.Cursor,
        pg_cursor: psycopg.Cursor[dict[str, Any]]
) -> None:
    """
    Test the transfer of genre data from SQLite to PostgreSQL.

    This function compares the data in the SQLite genre table with
    the data in the PostgreSQL genre table to ensure they match.

    Args:
        sqlite_cursor: SQLite cursor for reading data
        pg_cursor: PostgreSQL cursor for reading data
    """
    sqlite_cursor.execute('SELECT * FROM genre')

    while batch := sqlite_cursor.fetchmany(BATCH_SIZE):
        original_genre_batch = [Genre(**dict(genre)) for genre in batch]
        ids = [genre.id for genre in original_genre_batch]
        pg_cursor.execute(
            'SELECT id, '
            'name, '
            'description, '
            'created::text as created_at, '
            'modified::text as updated_at '
            'FROM genre '
            'WHERE id = ANY(%s)', [ids]
        )

        transferred_genre_batch = [
            Genre(**dict(genre_data))
            for genre_data in pg_cursor.fetchall()
        ]

        assert len(original_genre_batch) == len(transferred_genre_batch)
        assert original_genre_batch == transferred_genre_batch


def test_transfer_genre_film_work_table(
        sqlite_cursor: sqlite3.Cursor,
        pg_cursor: psycopg.Cursor[dict[str, Any]]
) -> None:
    """
    Test the transfer of genre film work data from SQLite to PostgreSQL.

    This function compares the data in the SQLite genre film work table with
    the data in the PostgreSQL genre film work table to ensure they match.

    Args:
        sqlite_cursor: SQLite cursor for reading data
        pg_cursor: PostgreSQL cursor for reading data
    """
    sqlite_cursor.execute('SELECT * FROM genre_film_work')

    while batch := sqlite_cursor.fetchmany(BATCH_SIZE):
        original_genre_film_work_batch = [
            GenreFilmWork(**dict(genre_film_work))
            for genre_film_work in batch
        ]
        ids = [
            genre_film_work_batch.id
            for genre_film_work_batch in original_genre_film_work_batch
        ]
        pg_cursor.execute(
            'SELECT id, '
            'genre_id::text, '
            'film_work_id::text, '
            'created::text as created_at '
            'FROM genre_film_work '
            'WHERE id = ANY(%s)', [ids]
        )

        transferred_genre_film_work_batch = [
            GenreFilmWork(**dict(genre_film_work_data))
            for genre_film_work_data in pg_cursor.fetchall()
        ]

        assert (
            len(original_genre_film_work_batch)
            == len(transferred_genre_film_work_batch)
        )
        assert (
            original_genre_film_work_batch
            == transferred_genre_film_work_batch
        )


def test_transfer_person_table(
        sqlite_cursor: sqlite3.Cursor,
        pg_cursor: psycopg.Cursor[dict[str, Any]]
) -> None:
    """
    Test the transfer of person data from SQLite to PostgreSQL.

    This function compares the data in the SQLite person table with
    the data in the PostgreSQL person table to ensure they match.

    Args:
        sqlite_cursor: SQLite cursor for reading data
        pg_cursor: PostgreSQL cursor for reading data
    """
    sqlite_cursor.execute('SELECT * FROM person  ORDER BY created_at')

    while batch := sqlite_cursor.fetchmany(BATCH_SIZE):
        original_person_batch = [
            Person(**dict(person))
            for person in batch
        ]
        ids = [
            person_batch.id
            for person_batch in original_person_batch
        ]
        pg_cursor.execute(
            'SELECT id, '
            'full_name, '
            'created::text as created_at, '
            'modified::text as updated_at '
            'FROM person '
            'WHERE id = ANY(%s) '
            'ORDER BY created', [ids]
        )

        transferred_person_batch = [
            Person(**dict(person_data))
            for person_data in pg_cursor.fetchall()
        ]

        assert (
            len(original_person_batch)
            == len(transferred_person_batch)
        )
        assert (
            original_person_batch
            == transferred_person_batch
        )


def test_transfer_person_film_work_table(
        sqlite_cursor: sqlite3.Cursor,
        pg_cursor: psycopg.Cursor[dict[str, Any]]
) -> None:
    """
    Test the transfer of person film work data from SQLite to PostgreSQL.

    This function compares the data in the SQLite person film work table with
    the data in the PostgreSQL person film work table to ensure they match.

    Args:
        sqlite_cursor: SQLite cursor for reading data
        pg_cursor: PostgreSQL cursor for reading data
    """
    sqlite_cursor.execute('SELECT * FROM person_film_work')

    while batch := sqlite_cursor.fetchmany(BATCH_SIZE):
        original_person_film_work_batch = [
            PersonFilmWork(**dict(person_film_work))
            for person_film_work in batch
        ]
        ids = [
            person_film_work_batch.id
            for person_film_work_batch in original_person_film_work_batch
        ]
        pg_cursor.execute(
            'SELECT id, '
            'film_work_id::text, '
            'person_id::text, '
            'role, '
            'created::text as created_at '
            'FROM person_film_work '
            'WHERE id = ANY(%s)', [ids]
        )

        transferred_person_film_work_batch = [
            PersonFilmWork(**dict(person_film_work_data))
            for person_film_work_data in pg_cursor.fetchall()
        ]

        assert (
            len(original_person_film_work_batch)
            == len(transferred_person_film_work_batch)
        )
        assert (
            original_person_film_work_batch
            == transferred_person_film_work_batch
        )


if __name__ == '__main__':
    with (
        closing(sqlite3.connect(sqlite_db_path)) as sqlite_conn,
        closing(psycopg.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port,
            options=OPTIONS
        )) as pg_conn
    ):
        sqlite_conn.row_factory = sqlite3.Row
        with (
            closing(sqlite_conn.cursor()) as sqlite_cur,
            closing(pg_conn.cursor()) as pg_write_cur,
            closing(pg_conn.cursor(row_factory=dict_row)) as pg_read_cur
        ):
            load_data(sqlite_cur, pg_write_cur, tables_maps)
            pg_conn.commit()

            test_transfer_film_work_table(sqlite_cur, pg_read_cur)
            test_transfer_genre_table(sqlite_cur, pg_read_cur)
            test_transfer_genre_film_work_table(sqlite_cur, pg_read_cur)
            test_transfer_person_table(sqlite_cur, pg_read_cur)
            test_transfer_person_film_work_table(sqlite_cur, pg_read_cur)

    print('🎉 Данные успешно перенесены !!!')
