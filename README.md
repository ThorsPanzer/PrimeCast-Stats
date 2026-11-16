# PrimeCast Stats

Local, browser-based League of Legends broadcast overlays for OBS Studio.

**Overlays:** Pregame SoloQ role comparisons, Postgame match overview, and Head-to-Head player comparisons. Includes team logos, live settings, cached match statistics, objective markers, and custom color/font controls.

## Requirements

- Windows 10/11, Python 3.10+ (standard library only), OBS Studio with Browser Source.
- An individual Riot Games API key from the [Riot Developer Portal](https://developer.riotgames.com/).
- Internet connectivity for Riot API queries and optional artwork/font assets.

## Run locally

1. Download this repository and extract it.
2. Run `START.cmd` or `python app.py --open-browser`.
3. Open `http://127.0.0.1:8767/` and enter your own API key under **Settings**.
4. Enter Riot IDs or OP.GG multisearch URLs for the two teams. Save, then start the statistics scan.
5. Add these OBS Browser Sources at **1920 × 1080**:
   - Pregame: `http://127.0.0.1:8767/pregame`
   - Postgame: `http://127.0.0.1:8767/postgame`
   - Head-to-Head: `http://127.0.0.1:8767/head2head`

Only run one copy per computer; port 8767 must be available. The app binds to `127.0.0.1` and does not expose a network service.

## Privacy and data

Your API key is stored locally in `config.json`. **Never commit or share this file.** SQLite caches are stored locally in `primecast.sqlite3`. The `.gitignore` excludes them by default. Every user must provide their own valid API key. Do not redistribute a personal key.

Rank and role icons, champion images, and some other visuals may be fetched from third-party content servers. Their availability is not guaranteed. The application is not affiliated with Riot Games, Prime League, OP.GG, or CommunityDragon.

## License

The original PrimeCast application code is distributed under **GNU GPL version 3**. See [LICENSE](LICENSE). GPLv3 allows copying, modification, distribution, and **commercial sale** subject to its terms. Free downloads and monetized streams are allowed. See [THIRD_PARTY.md](THIRD_PARTY.md) for third-party asset considerations.

**Riot Games disclaimer:** PrimeCast Stats isn't endorsed by Riot Games and doesn't reflect the views or opinions of Riot Games or anyone officially involved in producing or managing Riot Games properties. Riot Games, and all associated properties are trademarks or registered trademarks of Riot Games, Inc.

## Updates

Back up `config.json` and `primecast.sqlite3` before updating. Keep custom assets under `static/logos/`, `static/lanes/`, and `static/cached_rank_icons/`. See [CHANGELOG.txt](CHANGELOG.txt).
