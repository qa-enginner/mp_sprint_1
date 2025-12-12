"""
Django models for the movies application.

This module defines the data models for the movies application, including
models for films, genres, people, and their relationships. All models
inherit from Django's base Model class and use PostgreSQL-specific
features for optimal database design.
"""
import uuid
from django.db import models
from django.core.validators import MinValueValidator, MaxValueValidator
from django.utils.translation import gettext_lazy as _


class TimeStampedMixin(models.Model):
    """
    Abstract base class that provides timestamp fields for creation and
    modification.

    This mixin adds automatically managed timestamp fields to any model
    that inherits from it. The 'created' field is set only once when the
    object is first created. The 'modified' field is updated every time
    the object is saved.
    """
    created = models.DateTimeField(auto_now_add=True)
    modified = models.DateTimeField(auto_now=True)

    class Meta:
        abstract = True


class UUIDMixin(models.Model):
    """
    Abstract base class that provides a UUID primary key field.

    This mixin replaces the default integer primary key with a UUID field
    that is automatically generated when a new instance is created. The
    UUID is not editable and serves as the primary key for the model.
    """
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)

    class Meta:
        abstract = True


class Genre(UUIDMixin, TimeStampedMixin):
    """
    Model representing a genre of film or TV show.

    Each genre has a unique name and an optional description. The model
    inherits UUID primary key and timestamp fields from its mixins.
    """
    name = models.CharField(_('name'), unique=True, max_length=255)
    description = models.TextField(_('description'), blank=True, null=True)

    class Meta:
        db_table = "content\".\"genre"
        verbose_name = _('Genre')
        verbose_name_plural = _('Genres')

    def __str__(self):
        return self.name


class Person(UUIDMixin, TimeStampedMixin):
    """
    Model representing a person (actor, director, etc.) in the film industry.

    Each person is identified by their full name. The model inherits UUID
    primary key and timestamp fields from its mixins.
    """
    full_name = models.CharField(_('full_name'), max_length=255)

    class Meta:
        db_table = "content\".\"person"
        verbose_name = _('Actor')
        verbose_name_plural = _('Actors')

    def __str__(self):
        return self.full_name


class FilmWork(UUIDMixin, TimeStampedMixin):
    """
    Model representing a film or TV show work.

    This model contains all the information about a film or TV show,
    including its title, description, creation date, type (movie or
    TV show), rating, associated genres, and people involved. The
    model inherits UUID primary key and timestamp fields from its
    mixins.
    """
    title = models.CharField(_('title'), max_length=255)
    description = models.TextField(_('description'), blank=True, null=True)
    creation_date = models.DateField(_('creation_date'), blank=True, null=True)
    type = models.CharField(_('type'), max_length=255,
                            choices=[
                                ('movie', 'movie'), ('tv_show', 'tv_show')])
    rating = models.FloatField(_('rating'), blank=True,
                               validators=[MinValueValidator(0),
                                           MaxValueValidator(100)])
    genres = models.ManyToManyField(Genre, through='GenreFilmWork')
    person = models.ManyToManyField(Person, through='PersonFilmWork')

    class Meta:
        db_table = "content\".\"film_work"
        verbose_name = _('FilmWork')
        verbose_name_plural = _('FilmWorks')

    def __str__(self):
        return self.title


class GenreFilmWork(UUIDMixin):
    """
    Model representing the many-to-many relationship between films and genres.

    This intermediate model connects FilmWork and Genre models, allowing
    films to have multiple genres and genres to be associated with
    multiple films. It includes a timestamp for when the relationship
    was created.
    """
    film_work = models.ForeignKey('FilmWork', on_delete=models.CASCADE)
    genre = models.ForeignKey('Genre', on_delete=models.CASCADE)
    created = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "content\".\"genre_film_work"
        verbose_name = _('Film Genre')
        verbose_name_plural = _('Film Genres')
        unique_together = ('film_work', 'genre')


class PersonFilmWork(UUIDMixin):
    """
    Model representing the many-to-many relationship between films and people.

    This intermediate model connects FilmWork and Person models, allowing
    films to have multiple people associated with them and people to be
    associated with multiple films. It includes the role of the person
    in the film and a timestamp for when the relationship was created.
    """
    film_work = models.ForeignKey('FilmWork', on_delete=models.CASCADE)
    person = models.ForeignKey('Person', on_delete=models.CASCADE)
    role = models.TextField('role')
    created = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "content\".\"person_film_work"
        verbose_name = _('Person in FilmWork')
        verbose_name_plural = _('Person in FilmWorks')
        unique_together = ('film_work', 'person', 'role')
