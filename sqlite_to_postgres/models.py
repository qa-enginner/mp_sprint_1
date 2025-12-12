"""
Data models for the SQLite to PostgreSQL migration tool.

This module defines data classes that represent the database entities
used in the migration process from SQLite to PostgreSQL.
Each class corresponds to a table in the database schema.

Classes:
    FilmWork: Represents a film work entity
    Genre: Represents a genre entity
    GenreFilmWork: Represents the relationship between genres and film works
    Person: Represents a person entity
    PersonFilmWork: Represents the relationship between persons and film works
"""

import dataclasses

import datetime as dt

from dataclasses import dataclass
from uuid import UUID

from typing import Any


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
    created_at: dt.datetime
    updated_at: dt.datetime

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
