"""Script for transferring data from SQLite to PostgreSQL."""
import os
import logging
import datetime as dt
import sqlite3
import dataclasses

from uuid import UUID
from contextlib import closing
from dataclasses import astuple, dataclass
from typing import Any, Generator

import psycopg
from psycopg.rows import dict_row
from dotenv import load_dotenv


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


@dataclasses.dataclass(init=False)
class FilmWork():
    """
    Data class representing a film work entity.

    Attributes:
        id: Unique identifier
        title: Title of the film
        description: Description of the film
        creation_date: Date when the film was created
        rating: Rating of the film
        type: Type of the film work
        created_at: Timestamp when the record was created
        updated_at: Timestamp when the record was last updated
    """

    id: UUID
    title: str
    description: str
    creation_date: dt.datetime
    rating: float
    type: str

    def __init__(self: 'FilmWork', **kwargs: dict[str, Any]) -> None:
        """
        Initialize FilmWork instance.

        Args:
            **kwargs: Key-value pairs to initialize the instance attributes
        """
        names = {f.name for f in dataclasses.fields(self)}
        for k, v in kwargs.items():
            if k in names:
                setattr(self, k, v)
        if isinstance(self.id, str):
            self.id = UUID(self.id)


@dataclass
class Genre():
    """
    Data class representing a genre entity.

    Attributes:
        id: Unique identifier for the genre
        name: Name of the genre
        description: Description of the genre
        created_at: Timestamp when the record was created
        updated_at: Timestamp when the record was last updated
    """

    id: UUID
    name: str
    description: str
    created_at: dt.datetime
    updated_at: dt.datetime

    def __post_init__(self: 'Genre') -> None:
        """Post-initialization method converted string ID to UUID if needed."""
        if isinstance(self.id, str):
            self.id = UUID(self.id)


@dataclass
class GenreFilmWork():
    """
    Data class representing the relationship between genres and film works.

    Attributes:
        id: Unique identifier for the relationship
        genre_id: Identifier of the genre
        film_work_id: Identifier of the film work
        created_at: Timestamp when the record was created
    """

    id: UUID
    genre_id: UUID
    film_work_id: UUID
    created_at: dt.datetime

    def __post_init__(self: 'GenreFilmWork') -> None:
        """Post-initialization method converted string ID to UUID if needed."""
        if isinstance(self.id, str):
            self.id = UUID(self.id)


@dataclass
class Person():
    """
    Data class representing a person entity.

    Attributes:
        id: Unique identifier for the person
        full_name: Full name of the person
        created_at: Timestamp when the record was created
        updated_at: Timestamp when the record was last updated
    """

    id: UUID
    full_name: str
    created_at: dt.datetime
    updated_at: dt.datetime

    def __post_init__(self: 'Person') -> None:
        """Post-initialization method converted string ID to UUID if needed."""
        if isinstance(self.id, str):
            self.id = UUID(self.id)


@dataclass
class PersonFilmWork():
    """
    Data class representing the relationship between persons and film works.

    Attributes:
        id: Unique identifier for the relationship
        film_work_id: Identifier of the film work
        person_id: Identifier of the person
        role: Role of the person in the film work
        created_at: Timestamp when the record was created
    """

    id: UUID
    film_work_id: UUID
    person_id: UUID
    role: str
    created_at: dt.datetime

    def __post_init__(self: 'PersonFilmWork') -> None:
        """Post-initialization method converted string ID to UUID if needed."""
        if isinstance(self.id, str):
            self.id = UUID(self.id)


def transform_data_film_work(
        sqlite_cursor: sqlite3.Cursor
) -> Generator[list[FilmWork], None, None]:
    """
    Transform film work data from SQLite to FilmWork objects.

    Args:
        sqlite_cursor: SQLite cursor for database operations

    Yields:
        Generator[list[Genre], None, None]: Batch of FilmWork objects
    """
    for batch in extract_data(sqlite_cursor, 'film_work'):
        yield [FilmWork(**dict(film_work)) for film_work in batch]


def transform_data_genre(
        sqlite_cursor: sqlite3.Cursor
) -> Generator[list[Genre], None, None]:
    """
    Transform genre data from SQLite to Genre objects.

    Args:
        sqlite_cursor: SQLite cursor for database operations

    Yields:
        Generator[list[Genre], None, None]: Batch of Genre objects
    """
    for batch in extract_data(sqlite_cursor, 'genre'):
        yield [Genre(**dict(genre)) for genre in batch]


def transform_data_genre_film_work(
        sqlite_cursor: sqlite3.Cursor
) -> Generator[list[GenreFilmWork], None, None]:
    """
    Transform genre-film work relationship data from SQLite.

    Args:
        sqlite_cursor: SQLite cursor for database operations

    Yields:
        Generator[list[Genre], None, None]: Batch of GenreFilmWork objects
    """
    for batch in extract_data(sqlite_cursor, 'genre_film_work'):
        yield [
            GenreFilmWork(**dict(genre_film_work))
            for genre_film_work in batch
        ]


def transform_data_person_film_work(
        sqlite_cursor: sqlite3.Cursor
) -> Generator[list[PersonFilmWork], None, None]:
    """Transform person-film work data from SQLite.

    Args:
        sqlite_cursor: SQLite cursor for database operations

    Yields:
        Generator[list[PersonFilmWork], None, None]:
        Batch of PersonFilmWork objects
    """
    for batch in extract_data(sqlite_cursor, 'person_film_work'):
        yield [
            PersonFilmWork(**dict(person_film_work))
            for person_film_work in batch
        ]


def transform_data_person(
        sqlite_cursor: sqlite3.Cursor
) -> Generator[list[Person], None, None]:
    """
    Transform person data from SQLite to Person objects.

    Args:
        sqlite_cursor: SQLite cursor for database operations

    Yields:
        Generator[list[Person], None, None]: Batch of Person objects
    """
    for batch in extract_data(sqlite_cursor, 'person'):
        yield [Person(**dict(person)) for person in batch]


def load_data_film_work(
        sqlite_cursor: sqlite3.Cursor,
        pg_cursor: psycopg.Cursor[tuple[Any, ...]]
) -> None:
    """
    Load film_work data from SQLite to PostgreSQL database.

    Args:
        sqlite_cursor: SQLite cursor for reading data
        pg_cursor: PostgreSQL cursor for writing data
    """
    try:
        for batch in transform_data_film_work(sqlite_cursor):
            query = 'INSERT INTO film_work (' \
                'id, ' \
                'title, ' \
                'description, ' \
                'creation_date, ' \
                'rating, ' \
                'type, ' \
                'created, ' \
                'modified) ' \
                'VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW()) ' \
                'ON CONFLICT (id) DO NOTHING'
            batch_as_tuples = [astuple(film_work) for film_work in batch]
            pg_cursor.executemany(query, batch_as_tuples)
            logger.info("Loaded %s film_work records", len(batch))
    except Exception as e:
        logger.error("Error loading film_work data: %s", e)
        raise


def load_data_genre(
        sqlite_cursor: sqlite3.Cursor,
        pg_cursor: psycopg.Cursor[tuple[Any, ...]]
) -> None:
    """
    Load genre data from SQLite to PostgreSQL database.

    Args:
        sqlite_cursor: SQLite cursor for reading data
        pg_cursor: PostgreSQL cursor for writing data
    """
    try:
        for batch in transform_data_genre(sqlite_cursor):
            query = 'INSERT INTO genre (' \
                'id, ' \
                'name, ' \
                'description, ' \
                'created, ' \
                'modified) ' \
                'VALUES (%s, %s, %s, %s, %s) ' \
                'ON CONFLICT (id) DO NOTHING'
            batch_as_tuples = [astuple(genre) for genre in batch]
            pg_cursor.executemany(query, batch_as_tuples)
            logger.info("Loaded %s genre records", len(batch))
    except Exception as e:
        logger.error("Error loading genre data: %s", e)
        raise


def load_data_genre_film_work(
        sqlite_cursor: sqlite3.Cursor,
        pg_cursor: psycopg.Cursor[tuple[Any, ...]]
) -> None:
    """
    Load genre_film_work data from SQLite to PostgreSQL database.

    Args:
        sqlite_cursor: SQLite cursor for reading data
        pg_cursor: PostgreSQL cursor for writing data
    """
    try:
        for batch in transform_data_genre_film_work(sqlite_cursor):
            query = 'INSERT INTO genre_film_work (' \
                'id, ' \
                'genre_id, ' \
                'film_work_id, ' \
                'created) ' \
                'VALUES (%s, %s, %s, %s) ' \
                'ON CONFLICT (id) DO NOTHING'
            batch_as_tuples = [astuple(genre_film_work) for genre_film_work
                               in batch]
            pg_cursor.executemany(query, batch_as_tuples)
            logger.info("Loaded %s genre_film_work records", len(batch))
    except Exception as e:
        logger.error("Error loading genre_film_work data: %s", e)
        raise


def load_data_person(
        sqlite_cursor: sqlite3.Cursor,
        pg_cursor: psycopg.Cursor[tuple[Any, ...]]
) -> None:
    """
    Load person data from SQLite to PostgreSQL database.

    Args:
        sqlite_cursor: SQLite cursor for reading data
        pg_cursor: PostgreSQL cursor for writing data
    """
    try:
        for batch in transform_data_person(sqlite_cursor):
            query = 'INSERT INTO person (' \
                'id, ' \
                'full_name, ' \
                'created, ' \
                'modified) ' \
                'VALUES (%s, %s, %s, %s) ' \
                'ON CONFLICT (id) DO NOTHING'
            batch_as_tuples = [astuple(person) for person in batch]
            pg_cursor.executemany(query, batch_as_tuples)
            logger.info("Loaded %s person records", len(batch))
    except Exception as e:
        logger.error("Error loading person data: %s", e)
        raise


def load_data_person_film_work(
        sqlite_cursor: sqlite3.Cursor,
        pg_cursor: psycopg.Cursor[tuple[Any, ...]]
) -> None:
    """
    Load person_film_work data from SQLite to PostgreSQL database.

    Args:
        sqlite_cursor: SQLite cursor for reading data
        pg_cursor: PostgreSQL cursor for writing data
    """
    try:
        for batch in transform_data_person_film_work(sqlite_cursor):
            query = 'INSERT INTO person_film_work (' \
                'id, ' \
                'film_work_id, ' \
                'person_id, ' \
                'role, ' \
                'created) ' \
                'VALUES (%s, %s, %s, %s, %s) ' \
                'ON CONFLICT (id) DO NOTHING'
            batch_as_tuples = [astuple(person_film_work) for person_film_work
                               in batch]
            pg_cursor.executemany(query, batch_as_tuples)
            logger.info("Loaded %s person_film_work records", len(batch))
    except Exception as e:
        logger.error("Error loading person_film_work data: %s", e)
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
            'type '
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
            load_data_film_work(sqlite_cur, pg_write_cur)
            load_data_genre(sqlite_cur, pg_write_cur)
            load_data_genre_film_work(sqlite_cur, pg_write_cur)
            load_data_person(sqlite_cur, pg_write_cur)
            load_data_person_film_work(sqlite_cur, pg_write_cur)
            pg_conn.commit()

            test_transfer_film_work_table(sqlite_cur, pg_read_cur)
            test_transfer_genre_table(sqlite_cur, pg_read_cur)
            test_transfer_genre_film_work_table(sqlite_cur, pg_read_cur)
            test_transfer_person_table(sqlite_cur, pg_read_cur)
            test_transfer_person_film_work_table(sqlite_cur, pg_read_cur)

    print('🎉 Данные успешно перенесены !!!')
