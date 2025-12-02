# tests/unit/test_sync.py

import pytest
from unittest.mock import MagicMock, AsyncMock, patch
import asyncio

from spotify_to_tidal.sync import (
    get_albums_from_spotify,
    get_artists_from_spotify,
    sync_albums,
    sync_artists,
    simple,
    normalize,
    check_album_similarity,
)


# Test simple() function
def test_simple_removes_hyphens():
    assert simple("Song Name - Radio Edit") == "Song Name"


def test_simple_removes_parentheses():
    assert simple("Song Name (Remastered)") == "Song Name"


def test_simple_removes_brackets():
    assert simple("Song Name [Deluxe Edition]") == "Song Name"


def test_simple_handles_multiple_modifiers():
    assert simple("Song - Live (2020 Remaster) [Bonus]") == "Song"


def test_simple_preserves_simple_names():
    assert simple("Simple Song") == "Simple Song"


# Test normalize() function
def test_normalize_removes_accents():
    assert normalize("Café") == "Cafe"
    assert normalize("naïve") == "naive"
    assert normalize("Björk") == "Bjork"


def test_normalize_preserves_ascii():
    assert normalize("Hello World") == "Hello World"


# Test check_album_similarity() function
def test_check_album_similarity_matches_same_album():
    spotify_album = {
        'name': 'Abbey Road',
        'artists': [{'name': 'The Beatles'}],
    }
    tidal_album = MagicMock()
    tidal_album.name = 'Abbey Road'
    tidal_album.artists = [MagicMock(name='The Beatles')]
    tidal_album.artists[0].name = 'The Beatles'

    assert check_album_similarity(spotify_album, tidal_album) is True


def test_check_album_similarity_rejects_different_artist():
    spotify_album = {
        'name': 'Abbey Road',
        'artists': [{'name': 'The Beatles'}],
    }
    tidal_album = MagicMock()
    tidal_album.name = 'Abbey Road'
    tidal_album.artists = [MagicMock()]
    tidal_album.artists[0].name = 'Different Artist'

    assert check_album_similarity(spotify_album, tidal_album) is False


# Test get_albums_from_spotify()
@pytest.mark.asyncio
async def test_get_albums_from_spotify_single_page(mocker):
    mock_session = MagicMock()
    mock_session.current_user_saved_albums.return_value = {
        'items': [
            {'album': {'id': 'album1', 'name': 'Album 1'}},
            {'album': {'id': 'album2', 'name': 'Album 2'}},
        ],
        'next': None,
        'limit': 50,
        'total': 2,
    }

    result = await get_albums_from_spotify(mock_session)

    assert len(result) == 2
    # Results are reversed for chronological order
    assert result[0]['name'] == 'Album 2'
    assert result[1]['name'] == 'Album 1'


@pytest.mark.asyncio
async def test_get_albums_from_spotify_empty(mocker):
    mock_session = MagicMock()
    mock_session.current_user_saved_albums.return_value = {
        'items': [],
        'next': None,
        'limit': 50,
        'total': 0,
    }

    result = await get_albums_from_spotify(mock_session)

    assert len(result) == 0


# Test get_artists_from_spotify()
@pytest.mark.asyncio
async def test_get_artists_from_spotify_single_page(mocker):
    mock_session = MagicMock()
    mock_session.current_user_followed_artists.return_value = {
        'artists': {
            'items': [
                {'id': 'artist1', 'name': 'Artist 1'},
                {'id': 'artist2', 'name': 'Artist 2'},
            ],
            'next': None,
        }
    }

    result = await get_artists_from_spotify(mock_session)

    assert len(result) == 2
    assert result[0]['name'] == 'Artist 1'
    assert result[1]['name'] == 'Artist 2'


@pytest.mark.asyncio
async def test_get_artists_from_spotify_empty(mocker):
    mock_session = MagicMock()
    mock_session.current_user_followed_artists.return_value = {
        'artists': {
            'items': [],
            'next': None,
        }
    }

    result = await get_artists_from_spotify(mock_session)

    assert len(result) == 0


# Test sync_albums()
@pytest.mark.asyncio
async def test_sync_albums_adds_new_album(mocker):
    # Mock Spotify session
    mock_spotify = MagicMock()
    mock_spotify.current_user_saved_albums.return_value = {
        'items': [
            {'album': {'id': 'album1', 'name': 'Test Album', 'artists': [{'name': 'Test Artist'}]}},
        ],
        'next': None,
        'limit': 50,
        'total': 1,
    }

    # Mock Tidal session
    mock_tidal = MagicMock()
    mock_tidal.user.favorites.albums.return_value = []  # No existing albums

    # Mock search results
    mock_tidal_album = MagicMock()
    mock_tidal_album.id = 12345
    mock_tidal_album.name = 'Test Album'
    mock_tidal_album.artists = [MagicMock()]
    mock_tidal_album.artists[0].name = 'Test Artist'

    mock_tidal.search.return_value = {'albums': [mock_tidal_album]}

    # Mock the file writing
    mocker.patch('builtins.open', mocker.mock_open())

    await sync_albums(mock_spotify, mock_tidal, {})

    # Verify album was added
    mock_tidal.user.favorites.add_album.assert_called_once_with(12345)


@pytest.mark.asyncio
async def test_sync_albums_skips_existing_album(mocker):
    # Mock Spotify session
    mock_spotify = MagicMock()
    mock_spotify.current_user_saved_albums.return_value = {
        'items': [
            {'album': {'id': 'album1', 'name': 'Test Album', 'artists': [{'name': 'Test Artist'}]}},
        ],
        'next': None,
        'limit': 50,
        'total': 1,
    }

    # Mock Tidal session with existing album
    mock_existing_album = MagicMock()
    mock_existing_album.id = 12345
    mock_tidal = MagicMock()
    mock_tidal.user.favorites.albums.return_value = [mock_existing_album]

    # Mock search results
    mock_tidal_album = MagicMock()
    mock_tidal_album.id = 12345  # Same ID as existing
    mock_tidal_album.name = 'Test Album'
    mock_tidal_album.artists = [MagicMock()]
    mock_tidal_album.artists[0].name = 'Test Artist'

    mock_tidal.search.return_value = {'albums': [mock_tidal_album]}

    mocker.patch('builtins.open', mocker.mock_open())

    await sync_albums(mock_spotify, mock_tidal, {})

    # Verify album was NOT added (already exists)
    mock_tidal.user.favorites.add_album.assert_not_called()


# Test sync_artists()
@pytest.mark.asyncio
async def test_sync_artists_adds_new_artist(mocker):
    # Mock Spotify session
    mock_spotify = MagicMock()
    mock_spotify.current_user_followed_artists.return_value = {
        'artists': {
            'items': [{'id': 'artist1', 'name': 'Test Artist'}],
            'next': None,
        }
    }

    # Mock Tidal session
    mock_tidal = MagicMock()
    mock_tidal.user.favorites.artists.return_value = []  # No existing artists

    # Mock search results
    mock_tidal_artist = MagicMock()
    mock_tidal_artist.id = 12345
    mock_tidal_artist.name = 'Test Artist'

    mock_tidal.search.return_value = {'artists': [mock_tidal_artist]}

    mocker.patch('builtins.open', mocker.mock_open())

    await sync_artists(mock_spotify, mock_tidal, {})

    # Verify artist was added
    mock_tidal.user.favorites.add_artist.assert_called_once_with(12345)


@pytest.mark.asyncio
async def test_sync_artists_skips_existing_artist(mocker):
    # Mock Spotify session
    mock_spotify = MagicMock()
    mock_spotify.current_user_followed_artists.return_value = {
        'artists': {
            'items': [{'id': 'artist1', 'name': 'Test Artist'}],
            'next': None,
        }
    }

    # Mock Tidal session with existing artist
    mock_existing_artist = MagicMock()
    mock_existing_artist.id = 12345
    mock_tidal = MagicMock()
    mock_tidal.user.favorites.artists.return_value = [mock_existing_artist]

    # Mock search results
    mock_tidal_artist = MagicMock()
    mock_tidal_artist.id = 12345  # Same ID as existing
    mock_tidal_artist.name = 'Test Artist'

    mock_tidal.search.return_value = {'artists': [mock_tidal_artist]}

    mocker.patch('builtins.open', mocker.mock_open())

    await sync_artists(mock_spotify, mock_tidal, {})

    # Verify artist was NOT added (already exists)
    mock_tidal.user.favorites.add_artist.assert_not_called()


@pytest.mark.asyncio
async def test_sync_artists_handles_not_found(mocker):
    # Mock Spotify session
    mock_spotify = MagicMock()
    mock_spotify.current_user_followed_artists.return_value = {
        'artists': {
            'items': [{'id': 'artist1', 'name': 'Unknown Artist'}],
            'next': None,
        }
    }

    # Mock Tidal session
    mock_tidal = MagicMock()
    mock_tidal.user.favorites.artists.return_value = []

    # Mock empty search results
    mock_tidal.search.return_value = {'artists': []}

    mock_file = mocker.patch('builtins.open', mocker.mock_open())

    await sync_artists(mock_spotify, mock_tidal, {})

    # Verify artist was NOT added and file was written
    mock_tidal.user.favorites.add_artist.assert_not_called()
    mock_file.assert_called_with("artists not found.txt", "a", encoding="utf-8")
