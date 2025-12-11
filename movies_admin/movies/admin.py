"""Admin configuration for the movies application.

This module defines the Django admin interfaces for movie-related models,
including genres, persons, and film works, along with their relationships.
"""

from django.contrib import admin
from .models import Genre, FilmWork, GenreFilmWork, Person, PersonFilmWork


@admin.register(Genre)
class GenreAdmin(admin.ModelAdmin):
    """Admin interface for managing Genre objects.

    Displays genres with their names and descriptions, and allows
    searching by name.
    """
    list_display = ('name', 'description')
    search_fields = ('name',)


@admin.register(Person)
class PersonAdmin(admin.ModelAdmin):
    """Admin interface for managing Person objects.

    Allows searching by full name.
    """
    search_fields = ('full_name',)


class GenreFilmWorkInline(admin.TabularInline):
    """Inline interface for managing Genre-FilmWork relationships."""
    model = GenreFilmWork


class PersonFilmWorkInline(admin.TabularInline):
    """Inline interface for managing Person-FilmWork relationships."""
    model = PersonFilmWork


@admin.register(FilmWork)
class FilmWorkAdmin(admin.ModelAdmin):
    """Admin interface for managing FilmWork objects.

    Displays film works with their titles, types, creation dates, and ratings.
    Provides filtering by type and searching by title, description, and ID.
    Includes inline interfaces for managing genre and person relationships.
    """
    inlines = (GenreFilmWorkInline, PersonFilmWorkInline)

    list_display = ('title', 'type', 'creation_date', 'rating')

    list_filter = ('type',)

    search_fields = ('title', 'description', 'id')
