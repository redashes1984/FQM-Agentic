# FQM-Agentic

QQ Music collection → decrypted FLAC/MP3 library → Navidrome ingestion pipeline, packaged as a single stateless Docker image.

Pipeline (four stages, each stage a distinct component — evaluate independently, do not substitute):

```
acquire -> decrypt -> metadata -> archive
(go-music-dl) (um)   (QQ JSON / music_tag)   (NAS /mnt/user/Music/QQmusic)
```

## Components

| Stage | Tool | Form | Notes |
|---|---|---|---|
| Acquire | [go-music-dl](https://github.com/guohuiyuan/go-music-dl) | Go static binary (official image base) | Native "My Playlists" reads QQ favorites + saved playlists; QR-scan cookie renewal; filename templates |
| Decrypt | um (unlock-music CLI) | Static binary in `bin/` | Extensions `.mflac .mgg .qmc* .tkm`; unencrypted files pass through untouched |
| Metadata | QQ structured JSON (primary) / `music_tag` smart_tag (legacy only) | pip package | QQ API returns title/singer/album/mid/duration together with the stream — no guessing needed |
| Ingest | Navidrome @ `:4533` | External container | `ND_SCANSCHEDULE=1m` picks up files from the shared mount; not packaged in this image |

## Modes

One entrypoint script, three modes sharing the same dedup DB, naming rules and archive layout:

- `sync` — pull favorites/playlists via go-music-dl API, embed metadata at download, archive. Wired to cron (twice daily).
- `import` — scan inbox for manually fetched files. Dedup **first**, match second.
- `backfill` — one-off normalization of legacy library (rename, absorb sidecar `.lrc`, re-embed tags). Manual trigger only, never cron.

## Naming convention

```
/music/                        <- FLAC lands at root
/music/MP3/                    <- MP3 in subdir
/music/OGG/                    <- OGG in subdir
artist - title.flac            <- {artist} - {name}.{ext}, multi-artist joined with "/"
```

Album and QQ `mid` do **not** go into filenames — they live in Vorbis comment / ID3 and the dedup DB. Duplicate titles are resolved through the DB, never by suffixing `name (2).flac`. Failure triage: `music_tag/failed/{unlock_failed,metadata_failed,tag_failed}`.

## Deployment

```bash
docker compose up -d   # NAS has no compose plugin: use the `docker run` form below
docker run -d --name qqmusic-sync --restart unless-stopped \
  -p 8080:8080 -e TZ=Asia/Shanghai -e QQMUSIC_DIR=/music \
  -v /mnt/user/Music/QQmusic:/music -v /mnt/user/appdata/qqmusic-sync/data:/home/appuser/data \
  nova/qqmusic-sync:latest music-dl web --port 8080 --no-browser
```

- Container gets a dedicated LAN IP on the macvlan `eth1` network (`10.10.4.46`, music-stack block .40–.49), matching how navidrome (.42) / MusicTagWeb (.43) are attached. Formal deployments use the isolated IP; `-p 8080` is the fallback for hosts without macvlan.
- `QQMUSIC_DIR=/music` is the archive root environment override; compose binds host `/mnt/user/Music/QQmusic` to container `/music`, the same pair Navidrome mounts. Both containers see one truth source.
- go-music-dl's own download dir is persisted in its `settings.db` (`./data` volume): set once in Web settings to `/music`.

## Backup / rebuild surface

Only: `docker-compose.yml` + `./data` appdir. The image itself is stateless. Rebuild = `docker compose up -d`.

## Layout

```
FQM-Agentic/
├── README.md
├── Dockerfile
├── docker-compose.yml
├── bin/um                 # unlock-music CLI (static)
└── scripts/
    └── music_pipeline.py  # orchestrator: sync / import / backfill
```
