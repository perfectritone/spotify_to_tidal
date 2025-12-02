import yaml
import argparse
import sys

from . import sync as _sync
from . import auth as _auth
from . import backup as _backup

def main():
    parser = argparse.ArgumentParser(
        description='Sync Spotify playlists and favorites to Tidal',
        epilog='Examples:\n'
               '  Export Spotify data:  spotify_to_tidal --export backup.json\n'
               '  Import to Tidal:      spotify_to_tidal --import backup.json\n'
               '  Direct sync:          spotify_to_tidal',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('--config', default='config.yml', help='location of the config file')
    parser.add_argument('--uri', help='synchronize a specific URI instead of the one in the config')
    parser.add_argument('--sync-favorites', action=argparse.BooleanOptionalAction, help='synchronize the favorites')
    parser.add_argument('--sync-albums', action=argparse.BooleanOptionalAction, help='synchronize saved albums')
    parser.add_argument('--sync-artists', action=argparse.BooleanOptionalAction, help='synchronize followed artists')
    parser.add_argument('--export', metavar='FILE', help='export Spotify data to a local JSON file (no Tidal login required)')
    parser.add_argument('--import', dest='import_file', metavar='FILE', help='import from a local backup file to Tidal (no Spotify login required)')
    args = parser.parse_args()

    # Validate mutually exclusive options
    if args.export and args.import_file:
        sys.exit("Error: --export and --import cannot be used together")
    if args.export and args.uri:
        sys.exit("Error: --export and --uri cannot be used together")
    if args.import_file and args.uri:
        sys.exit("Error: --import and --uri cannot be used together")

    with open(args.config, 'r') as f:
        config = yaml.safe_load(f)

    # Handle export mode (Spotify only)
    if args.export:
        print("Opening Spotify session")
        spotify_session = _auth.open_spotify_session(config['spotify'])
        include_favorites = args.sync_favorites is None or args.sync_favorites
        include_albums = args.sync_albums is None or args.sync_albums
        include_artists = args.sync_artists is None or args.sync_artists
        _backup.export_wrapper(
            spotify_session, config, args.export,
            include_favorites, include_albums, include_artists
        )
        return

    # Handle import mode (Tidal only)
    if args.import_file:
        print("Opening Tidal session")
        tidal_session = _auth.open_tidal_session()
        if not tidal_session.check_login():
            sys.exit("Could not connect to Tidal")
        sync_favorites = args.sync_favorites is None or args.sync_favorites
        sync_albums = args.sync_albums is None or args.sync_albums
        sync_artists = args.sync_artists is None or args.sync_artists
        _backup.import_wrapper(
            tidal_session, args.import_file, config,
            sync_favorites, sync_albums, sync_artists
        )
        return

    # Standard sync mode (both Spotify and Tidal)
    print("Opening Spotify session")
    spotify_session = _auth.open_spotify_session(config['spotify'])
    print("Opening Tidal session")
    tidal_session = _auth.open_tidal_session()
    if not tidal_session.check_login():
        sys.exit("Could not connect to Tidal")
    # Determine what to sync based on arguments and config
    sync_favorites = None
    sync_albums = None
    sync_artists = None

    if args.uri:
        # if a playlist ID is explicitly provided as a command line argument then use that
        spotify_playlist = spotify_session.playlist(args.uri)
        tidal_playlists = _sync.get_tidal_playlists_wrapper(tidal_session)
        tidal_playlist = _sync.pick_tidal_playlist_for_spotify_playlist(spotify_playlist, tidal_playlists)
        _sync.sync_playlists_wrapper(spotify_session, tidal_session, [tidal_playlist], config)
        # only sync extras if command line argument explicitly passed
        sync_favorites = args.sync_favorites
        sync_albums = args.sync_albums
        sync_artists = args.sync_artists
    elif args.sync_favorites or args.sync_albums or args.sync_artists:
        # sync only the explicitly requested types
        sync_favorites = args.sync_favorites
        sync_albums = args.sync_albums
        sync_artists = args.sync_artists
    elif config.get('sync_playlists', None):
        # if the config contains a sync_playlists list of mappings then use that
        _sync.sync_playlists_wrapper(spotify_session, tidal_session, _sync.get_playlists_from_config(spotify_session, tidal_session, config), config)
        sync_favorites = args.sync_favorites is None and config.get('sync_favorites_default', True)
        sync_albums = args.sync_albums is None and config.get('sync_albums_default', False)
        sync_artists = args.sync_artists is None and config.get('sync_artists_default', False)
    else:
        # otherwise sync all the user playlists in the Spotify account and extras based on config
        _sync.sync_playlists_wrapper(spotify_session, tidal_session, _sync.get_user_playlist_mappings(spotify_session, tidal_session, config), config)
        sync_favorites = args.sync_favorites is None and config.get('sync_favorites_default', True)
        sync_albums = args.sync_albums is None and config.get('sync_albums_default', False)
        sync_artists = args.sync_artists is None and config.get('sync_artists_default', False)

    if sync_favorites:
        _sync.sync_favorites_wrapper(spotify_session, tidal_session, config)
    if sync_albums:
        _sync.sync_albums_wrapper(spotify_session, tidal_session, config)
    if sync_artists:
        _sync.sync_artists_wrapper(spotify_session, tidal_session, config)

if __name__ == '__main__':
    main()
    sys.exit(0)
