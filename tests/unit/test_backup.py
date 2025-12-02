# tests/unit/test_backup.py

import json
import pytest
import tempfile
from pathlib import Path

from spotify_to_tidal.backup import (
    _simplify_track,
    _simplify_playlist,
    load_backup,
    BACKUP_VERSION,
)


# Sample data fixtures
@pytest.fixture
def sample_spotify_track():
    """Sample Spotify track as returned by the API."""
    return {
        'id': 'track123',
        'name': 'Test Song',
        'duration_ms': 180000,
        'track_number': 5,
        'external_ids': {'isrc': 'USABC1234567'},
        'artists': [
            {'name': 'Artist One', 'id': 'artist1', 'extra_field': 'ignored'},
            {'name': 'Artist Two', 'id': 'artist2'},
        ],
        'album': {
            'name': 'Test Album',
            'id': 'album123',
            'artists': [{'name': 'Album Artist', 'id': 'albumartist1'}],
            'release_date': '2023-01-01',
        },
        'popularity': 75,
        'preview_url': 'https://example.com/preview',
    }


@pytest.fixture
def sample_playlist():
    """Sample Spotify playlist."""
    return {
        'id': 'playlist123',
        'name': 'My Playlist',
        'description': 'A test playlist',
        'owner': {'id': 'user123'},
        'tracks': {'total': 1},
    }


@pytest.fixture
def temp_backup_file():
    """Create a temporary file for backup testing."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        temp_path = f.name
    yield temp_path
    # Cleanup
    Path(temp_path).unlink(missing_ok=True)


# Test _simplify_track
def test_simplify_track_extracts_required_fields(sample_spotify_track):
    result = _simplify_track(sample_spotify_track)

    assert result['id'] == 'track123'
    assert result['name'] == 'Test Song'
    assert result['duration_ms'] == 180000
    assert result['track_number'] == 5
    assert result['external_ids'] == {'isrc': 'USABC1234567'}


def test_simplify_track_simplifies_artists(sample_spotify_track):
    result = _simplify_track(sample_spotify_track)

    assert len(result['artists']) == 2
    assert result['artists'][0] == {'name': 'Artist One'}
    assert result['artists'][1] == {'name': 'Artist Two'}
    # Extra fields should be stripped
    assert 'id' not in result['artists'][0]


def test_simplify_track_simplifies_album(sample_spotify_track):
    result = _simplify_track(sample_spotify_track)

    assert result['album']['name'] == 'Test Album'
    assert result['album']['artists'] == [{'name': 'Album Artist'}]
    # Extra fields should be stripped
    assert 'release_date' not in result['album']
    assert 'id' not in result['album']


def test_simplify_track_handles_missing_fields():
    minimal_track = {
        'id': 'track1',
        'name': 'Minimal',
    }
    result = _simplify_track(minimal_track)

    assert result['id'] == 'track1'
    assert result['name'] == 'Minimal'
    assert result['duration_ms'] is None
    assert result['external_ids'] == {}
    assert result['artists'] == []


def test_simplify_track_handles_empty_artists():
    track = {
        'id': 'track1',
        'name': 'No Artists',
        'artists': [],
        'album': {'name': 'Album', 'artists': []},
    }
    result = _simplify_track(track)

    assert result['artists'] == []
    assert result['album']['artists'] == []


# Test _simplify_playlist
def test_simplify_playlist_extracts_metadata(sample_playlist, sample_spotify_track):
    tracks = [sample_spotify_track]
    result = _simplify_playlist(sample_playlist, tracks)

    assert result['id'] == 'playlist123'
    assert result['name'] == 'My Playlist'
    assert result['description'] == 'A test playlist'
    assert len(result['tracks']) == 1


def test_simplify_playlist_simplifies_tracks(sample_playlist, sample_spotify_track):
    tracks = [sample_spotify_track]
    result = _simplify_playlist(sample_playlist, tracks)

    # Track should be simplified - extra fields removed
    simplified_track = result['tracks'][0]
    assert 'popularity' not in simplified_track
    assert 'preview_url' not in simplified_track
    assert simplified_track['id'] == 'track123'


def test_simplify_playlist_handles_empty_description(sample_spotify_track):
    playlist = {
        'id': 'playlist1',
        'name': 'No Description',
    }
    result = _simplify_playlist(playlist, [sample_spotify_track])

    assert result['description'] == ''


def test_simplify_playlist_handles_empty_tracks(sample_playlist):
    result = _simplify_playlist(sample_playlist, [])

    assert result['tracks'] == []


# Test load_backup
def test_load_backup_valid_file(temp_backup_file):
    backup_data = {
        'version': BACKUP_VERSION,
        'exported_at': '2024-01-15T10:30:00Z',
        'spotify_user': 'testuser',
        'playlists': [
            {'id': 'pl1', 'name': 'Playlist 1', 'description': '', 'tracks': []}
        ],
        'favorites': [],
    }

    with open(temp_backup_file, 'w') as f:
        json.dump(backup_data, f)

    result = load_backup(temp_backup_file)

    assert result['version'] == BACKUP_VERSION
    assert result['spotify_user'] == 'testuser'
    assert len(result['playlists']) == 1
    assert result['playlists'][0]['name'] == 'Playlist 1'


def test_load_backup_missing_version(temp_backup_file):
    backup_data = {
        'playlists': [],
    }

    with open(temp_backup_file, 'w') as f:
        json.dump(backup_data, f)

    with pytest.raises(ValueError, match="missing version field"):
        load_backup(temp_backup_file)


def test_load_backup_future_version(temp_backup_file):
    backup_data = {
        'version': BACKUP_VERSION + 1,
        'playlists': [],
    }

    with open(temp_backup_file, 'w') as f:
        json.dump(backup_data, f)

    with pytest.raises(ValueError, match="newer than supported"):
        load_backup(temp_backup_file)


def test_load_backup_missing_playlists(temp_backup_file):
    backup_data = {
        'version': BACKUP_VERSION,
    }

    with open(temp_backup_file, 'w') as f:
        json.dump(backup_data, f)

    with pytest.raises(ValueError, match="missing playlists field"):
        load_backup(temp_backup_file)


def test_load_backup_with_tracks(temp_backup_file):
    backup_data = {
        'version': BACKUP_VERSION,
        'exported_at': '2024-01-15T10:30:00Z',
        'spotify_user': 'testuser',
        'playlists': [
            {
                'id': 'pl1',
                'name': 'Test Playlist',
                'description': 'Description',
                'tracks': [
                    {
                        'id': 'track1',
                        'name': 'Song 1',
                        'duration_ms': 200000,
                        'track_number': 1,
                        'external_ids': {'isrc': 'TEST1234'},
                        'artists': [{'name': 'Artist'}],
                        'album': {'name': 'Album', 'artists': [{'name': 'Artist'}]},
                    }
                ],
            }
        ],
        'favorites': [
            {
                'id': 'fav1',
                'name': 'Favorite Song',
                'duration_ms': 180000,
                'track_number': 3,
                'external_ids': {},
                'artists': [{'name': 'Fav Artist'}],
                'album': {'name': 'Fav Album', 'artists': []},
            }
        ],
    }

    with open(temp_backup_file, 'w') as f:
        json.dump(backup_data, f)

    result = load_backup(temp_backup_file)

    assert len(result['playlists']) == 1
    assert len(result['playlists'][0]['tracks']) == 1
    assert result['playlists'][0]['tracks'][0]['name'] == 'Song 1'
    assert len(result['favorites']) == 1
    assert result['favorites'][0]['name'] == 'Favorite Song'
