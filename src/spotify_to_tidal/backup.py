#!/usr/bin/env python3
"""
Backup module for exporting Spotify data locally and importing to Tidal.

This enables a two-step migration process:
1. Export: Backup all Spotify playlists, favorites, albums, and artists to a local JSON file
2. Import: Read the backup file and sync to Tidal (without needing Spotify)
"""

import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Sequence

import spotipy
import tidalapi

from .sync import (
    get_playlists_from_spotify,
    get_tracks_from_spotify_playlist,
    get_tidal_playlists_wrapper,
    pick_tidal_playlist_for_spotify_playlist,
    populate_track_match_cache,
    search_new_tracks_on_tidal,
    get_tracks_for_new_tidal_playlist,
    get_all_playlist_tracks,
    repeat_on_request_error,
    _fetch_all_from_spotify_in_chunks,
    get_albums_from_spotify,
    get_artists_from_spotify,
    simple,
    normalize,
    check_album_similarity,
)
from .tidalapi_patch import (
    add_multiple_tracks_to_playlist,
    clear_tidal_playlist,
    get_all_favorites,
)
from .cache import track_match_cache
from .type import spotify as t_spotify
from tqdm import tqdm


# Current backup format version
BACKUP_VERSION = 2


def _simplify_track(track: dict) -> dict:
    """Extract only the fields needed for Tidal matching from a Spotify track."""
    return {
        'id': track.get('id'),
        'name': track.get('name'),
        'duration_ms': track.get('duration_ms'),
        'track_number': track.get('track_number'),
        'external_ids': track.get('external_ids', {}),
        'artists': [{'name': a.get('name')} for a in track.get('artists', [])],
        'album': {
            'name': track.get('album', {}).get('name'),
            'artists': [{'name': a.get('name')} for a in track.get('album', {}).get('artists', [])],
        }
    }


def _simplify_playlist(playlist: dict, tracks: List[dict]) -> dict:
    """Extract playlist metadata and simplified tracks."""
    return {
        'id': playlist.get('id'),
        'name': playlist.get('name'),
        'description': playlist.get('description', ''),
        'tracks': [_simplify_track(t) for t in tracks],
    }


def _simplify_album(album: dict) -> dict:
    """Extract only the fields needed for Tidal matching from a Spotify album."""
    return {
        'id': album.get('id'),
        'name': album.get('name'),
        'artists': [{'name': a.get('name')} for a in album.get('artists', [])],
    }


def _simplify_artist(artist: dict) -> dict:
    """Extract only the fields needed for Tidal matching from a Spotify artist."""
    return {
        'id': artist.get('id'),
        'name': artist.get('name'),
    }


async def _get_spotify_favorites(spotify_session: spotipy.Spotify) -> List[dict]:
    """Fetch all favorite tracks from Spotify."""
    _get_favorite_tracks = lambda offset: spotify_session.current_user_saved_tracks(offset=offset)
    tracks = await repeat_on_request_error(_fetch_all_from_spotify_in_chunks, _get_favorite_tracks)
    tracks.reverse()
    return tracks


async def export_spotify_data(
    spotify_session: spotipy.Spotify,
    config: dict,
    output_path: str,
    include_favorites: bool = True,
    include_albums: bool = True,
    include_artists: bool = True,
) -> None:
    """
    Export all Spotify playlists, favorites, albums, and artists to a local JSON file.

    Args:
        spotify_session: Authenticated Spotify session
        config: Configuration dictionary
        output_path: Path to write the backup JSON file
        include_favorites: Whether to include liked songs in the backup
        include_albums: Whether to include saved albums in the backup
        include_artists: Whether to include followed artists in the backup
    """
    print("Starting Spotify data export...")

    # Get user info
    user_info = spotify_session.current_user()
    username = user_info.get('id', 'unknown')

    # Fetch all playlists
    print("Fetching playlists from Spotify...")
    playlists = await get_playlists_from_spotify(spotify_session, config)
    print(f"Found {len(playlists)} playlists")

    # Fetch tracks for each playlist
    exported_playlists = []
    for playlist in tqdm(playlists, desc="Exporting playlists"):
        tracks = await get_tracks_from_spotify_playlist(spotify_session, playlist)
        exported_playlists.append(_simplify_playlist(playlist, tracks))

    # Fetch favorites if requested
    exported_favorites = []
    if include_favorites:
        print("Fetching favorite tracks from Spotify...")
        favorites = await _get_spotify_favorites(spotify_session)
        exported_favorites = [_simplify_track(t) for t in favorites]
        print(f"Found {len(exported_favorites)} favorite tracks")

    # Fetch albums if requested
    exported_albums = []
    if include_albums:
        print("Fetching saved albums from Spotify...")
        albums = await get_albums_from_spotify(spotify_session)
        exported_albums = [_simplify_album(a) for a in albums]
        print(f"Found {len(exported_albums)} saved albums")

    # Fetch artists if requested
    exported_artists = []
    if include_artists:
        print("Fetching followed artists from Spotify...")
        artists = await get_artists_from_spotify(spotify_session)
        exported_artists = [_simplify_artist(a) for a in artists]
        print(f"Found {len(exported_artists)} followed artists")

    # Build the backup structure
    backup_data = {
        'version': BACKUP_VERSION,
        'exported_at': datetime.now(timezone.utc).isoformat(),
        'spotify_user': username,
        'playlists': exported_playlists,
        'favorites': exported_favorites,
        'albums': exported_albums,
        'artists': exported_artists,
    }

    # Calculate totals
    total_playlist_tracks = sum(len(p['tracks']) for p in exported_playlists)
    total_tracks = total_playlist_tracks + len(exported_favorites)

    # Write to file
    output_file = Path(output_path)
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(backup_data, f, indent=2, ensure_ascii=False)

    print(f"\nExport complete!")
    print(f"  Playlists: {len(exported_playlists)}")
    print(f"  Playlist tracks: {total_playlist_tracks}")
    print(f"  Favorite tracks: {len(exported_favorites)}")
    print(f"  Albums: {len(exported_albums)}")
    print(f"  Artists: {len(exported_artists)}")
    print(f"  Saved to: {output_file.absolute()}")


def load_backup(backup_path: str) -> dict:
    """
    Load and validate a backup file.

    Args:
        backup_path: Path to the backup JSON file

    Returns:
        Parsed backup data

    Raises:
        ValueError: If the backup file is invalid or incompatible
    """
    with open(backup_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Validate version
    version = data.get('version')
    if version is None:
        raise ValueError("Invalid backup file: missing version field")
    if version > BACKUP_VERSION:
        raise ValueError(f"Backup version {version} is newer than supported version {BACKUP_VERSION}")

    # Validate required fields
    if 'playlists' not in data:
        raise ValueError("Invalid backup file: missing playlists field")

    return data


async def sync_playlist_from_backup(
    tidal_session: tidalapi.Session,
    playlist_data: dict,
    tidal_playlist: tidalapi.Playlist | None,
    config: dict,
) -> None:
    """
    Sync a single playlist from backup data to Tidal.

    Args:
        tidal_session: Authenticated Tidal session
        playlist_data: Playlist data from backup file
        tidal_playlist: Existing Tidal playlist or None to create new
        config: Configuration dictionary
    """
    spotify_tracks: List[t_spotify.SpotifyTrack] = playlist_data['tracks']
    playlist_name = playlist_data['name']

    if len(spotify_tracks) == 0:
        print(f"Skipping empty playlist: '{playlist_name}'")
        return

    # Create Tidal playlist if it doesn't exist
    if tidal_playlist:
        old_tidal_tracks = await get_all_playlist_tracks(tidal_playlist)
    else:
        print(f"Creating new Tidal playlist: '{playlist_name}'")
        description = playlist_data.get('description', '')
        tidal_playlist = tidal_session.user.create_playlist(playlist_name, description)
        old_tidal_tracks = []

    # Match and search for tracks
    populate_track_match_cache(spotify_tracks, old_tidal_tracks)
    await search_new_tracks_on_tidal(tidal_session, spotify_tracks, playlist_name, config)
    new_tidal_track_ids = get_tracks_for_new_tidal_playlist(spotify_tracks)

    # Update the Tidal playlist
    old_tidal_track_ids = [t.id for t in old_tidal_tracks]
    if new_tidal_track_ids == old_tidal_track_ids:
        print(f"No changes to write for playlist: '{playlist_name}'")
    elif new_tidal_track_ids[:len(old_tidal_track_ids)] == old_tidal_track_ids:
        add_multiple_tracks_to_playlist(tidal_playlist, new_tidal_track_ids[len(old_tidal_track_ids):])
    else:
        clear_tidal_playlist(tidal_playlist)
        add_multiple_tracks_to_playlist(tidal_playlist, new_tidal_track_ids)


async def sync_favorites_from_backup(
    tidal_session: tidalapi.Session,
    favorites_data: List[dict],
    config: dict,
) -> None:
    """
    Sync favorites from backup data to Tidal.

    Args:
        tidal_session: Authenticated Tidal session
        favorites_data: List of favorite tracks from backup
        config: Configuration dictionary
    """
    if not favorites_data:
        print("No favorites in backup to sync")
        return

    spotify_tracks: List[t_spotify.SpotifyTrack] = favorites_data
    print(f"Syncing {len(spotify_tracks)} favorite tracks from backup...")

    # Get existing Tidal favorites
    print("Loading existing favorite tracks from Tidal...")
    old_tidal_tracks = await get_all_favorites(tidal_session.user.favorites, order='DATE')

    # Match and search for tracks
    populate_track_match_cache(spotify_tracks, old_tidal_tracks)
    await search_new_tracks_on_tidal(tidal_session, spotify_tracks, "Favorites", config)

    # Add new favorites to Tidal
    existing_favorite_ids = set([track.id for track in old_tidal_tracks])
    new_ids = []
    for spotify_track in spotify_tracks:
        if not spotify_track.get('id'):
            continue
        match_id = track_match_cache.get(spotify_track['id'])
        if match_id and match_id not in existing_favorite_ids:
            new_ids.append(match_id)

    if new_ids:
        for tidal_id in tqdm(new_ids, desc="Adding new tracks to Tidal favorites"):
            tidal_session.user.favorites.add_track(tidal_id)
    else:
        print("No new tracks to add to Tidal favorites")


async def sync_albums_from_backup(
    tidal_session: tidalapi.Session,
    albums_data: List[dict],
) -> None:
    """
    Sync albums from backup data to Tidal.

    Args:
        tidal_session: Authenticated Tidal session
        albums_data: List of albums from backup
    """
    if not albums_data:
        print("No albums in backup to sync")
        return

    print(f"Syncing {len(albums_data)} albums from backup...")

    # Get existing Tidal favorite albums to avoid duplicates
    print("Loading existing favorite albums from Tidal...")
    tidal_favorite_albums = set()
    try:
        tidal_albums = tidal_session.user.favorites.albums()
        for album in tidal_albums:
            tidal_favorite_albums.add(album.id)
    except Exception as e:
        print(f"Warning: Could not fetch existing Tidal albums: {e}")

    added_count = 0
    not_found = []

    for spotify_album in tqdm(albums_data, desc="Syncing albums to Tidal"):
        album_name = spotify_album['name']
        artist_name = spotify_album['artists'][0]['name'] if spotify_album.get('artists') else ''

        # Search for album on Tidal
        query = f"{simple(album_name)} {simple(artist_name)}"
        try:
            search_results = tidal_session.search(query, models=[tidalapi.album.Album])
            matched = False

            for tidal_album in search_results.get('albums', []):
                if check_album_similarity(spotify_album, tidal_album):
                    if tidal_album.id not in tidal_favorite_albums:
                        try:
                            tidal_session.user.favorites.add_album(tidal_album.id)
                            tidal_favorite_albums.add(tidal_album.id)
                            added_count += 1
                        except Exception as e:
                            print(f"Failed to add album '{album_name}': {e}")
                    matched = True
                    break

            if not matched:
                not_found.append(f"{artist_name} - {album_name}")
        except Exception as e:
            print(f"Error searching for album '{album_name}': {e}")
            not_found.append(f"{artist_name} - {album_name}")

    print(f"\nAlbum sync complete: {added_count} albums added to Tidal")
    if not_found:
        print(f"{len(not_found)} albums could not be found on Tidal")


async def sync_artists_from_backup(
    tidal_session: tidalapi.Session,
    artists_data: List[dict],
) -> None:
    """
    Sync artists from backup data to Tidal.

    Args:
        tidal_session: Authenticated Tidal session
        artists_data: List of artists from backup
    """
    if not artists_data:
        print("No artists in backup to sync")
        return

    print(f"Syncing {len(artists_data)} artists from backup...")

    # Get existing Tidal favorite artists to avoid duplicates
    print("Loading existing favorite artists from Tidal...")
    tidal_favorite_artists = set()
    try:
        tidal_artists = tidal_session.user.favorites.artists()
        for artist in tidal_artists:
            tidal_favorite_artists.add(artist.id)
    except Exception as e:
        print(f"Warning: Could not fetch existing Tidal artists: {e}")

    added_count = 0
    not_found = []

    for spotify_artist in tqdm(artists_data, desc="Syncing artists to Tidal"):
        artist_name = spotify_artist['name']

        # Search for artist on Tidal
        query = simple(artist_name)
        try:
            search_results = tidal_session.search(query, models=[tidalapi.artist.Artist])
            matched = False

            for tidal_artist in search_results.get('artists', []):
                # Match by normalized name comparison
                if normalize(simple(tidal_artist.name.lower())) == normalize(simple(artist_name.lower())):
                    if tidal_artist.id not in tidal_favorite_artists:
                        try:
                            tidal_session.user.favorites.add_artist(tidal_artist.id)
                            tidal_favorite_artists.add(tidal_artist.id)
                            added_count += 1
                        except Exception as e:
                            print(f"Failed to add artist '{artist_name}': {e}")
                    matched = True
                    break

            if not matched:
                not_found.append(artist_name)
        except Exception as e:
            print(f"Error searching for artist '{artist_name}': {e}")
            not_found.append(artist_name)

    print(f"\nArtist sync complete: {added_count} artists added to Tidal")
    if not_found:
        print(f"{len(not_found)} artists could not be found on Tidal")


async def import_from_backup(
    tidal_session: tidalapi.Session,
    backup_path: str,
    config: dict,
    sync_favorites: bool = True,
    sync_albums: bool = True,
    sync_artists: bool = True,
) -> None:
    """
    Import playlists, favorites, albums, and artists from a backup file to Tidal.

    Args:
        tidal_session: Authenticated Tidal session
        backup_path: Path to the backup JSON file
        config: Configuration dictionary
        sync_favorites: Whether to sync favorites from the backup
        sync_albums: Whether to sync albums from the backup
        sync_artists: Whether to sync artists from the backup
    """
    print(f"Loading backup from: {backup_path}")
    backup_data = load_backup(backup_path)

    print(f"Backup info:")
    print(f"  Exported at: {backup_data.get('exported_at', 'unknown')}")
    print(f"  Spotify user: {backup_data.get('spotify_user', 'unknown')}")
    print(f"  Playlists: {len(backup_data['playlists'])}")
    print(f"  Favorites: {len(backup_data.get('favorites', []))}")
    print(f"  Albums: {len(backup_data.get('albums', []))}")
    print(f"  Artists: {len(backup_data.get('artists', []))}")

    # Get existing Tidal playlists for matching
    print("\nFetching existing Tidal playlists...")
    tidal_playlists = get_tidal_playlists_wrapper(tidal_session)

    # Sync each playlist
    for playlist_data in backup_data['playlists']:
        playlist_name = playlist_data['name']
        tidal_playlist = tidal_playlists.get(playlist_name)

        if tidal_playlist:
            print(f"\nSyncing to existing Tidal playlist: '{playlist_name}'")
        else:
            print(f"\nWill create new Tidal playlist: '{playlist_name}'")

        await sync_playlist_from_backup(tidal_session, playlist_data, tidal_playlist, config)

    # Sync favorites if requested
    if sync_favorites and backup_data.get('favorites'):
        print("\n" + "=" * 50)
        await sync_favorites_from_backup(tidal_session, backup_data['favorites'], config)

    # Sync albums if requested
    if sync_albums and backup_data.get('albums'):
        print("\n" + "=" * 50)
        await sync_albums_from_backup(tidal_session, backup_data['albums'])

    # Sync artists if requested
    if sync_artists and backup_data.get('artists'):
        print("\n" + "=" * 50)
        await sync_artists_from_backup(tidal_session, backup_data['artists'])

    print("\nImport complete!")


def export_wrapper(
    spotify_session: spotipy.Spotify,
    config: dict,
    output_path: str,
    include_favorites: bool = True,
    include_albums: bool = True,
    include_artists: bool = True,
):
    """Wrapper to run export in asyncio event loop."""
    asyncio.run(export_spotify_data(
        spotify_session, config, output_path,
        include_favorites, include_albums, include_artists
    ))


def import_wrapper(
    tidal_session: tidalapi.Session,
    backup_path: str,
    config: dict,
    sync_favorites: bool = True,
    sync_albums: bool = True,
    sync_artists: bool = True,
):
    """Wrapper to run import in asyncio event loop."""
    asyncio.run(import_from_backup(
        tidal_session, backup_path, config,
        sync_favorites, sync_albums, sync_artists
    ))
