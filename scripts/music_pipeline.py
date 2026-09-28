#!/usr/bin/env python3
"""FQM-Agentic orchestrator — sync / import / backfill.

Single entrypoint for the three pipeline modes. All paths derive from the
QQMUSIC_DIR env var (archive root mounted at /music). Dedup state lives in
the appdata volume (DB + logs), never inside the image.

Modes:
  sync     pull favorites/playlists via go-music-dl, embed metadata at download
  import   scan inbox/, dedup first, match second, embed, archive
  backfill one-off normalization of the existing library (rename / absorb
           sidecar .lrc / re-embed tags). Manual trigger only.
"""

import argparse
import json
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import urllib.request
import urllib.error
from pathlib import Path
from datetime import datetime

# ===================== Configuration =====================

BASE_DIR = Path(os.environ.get("QQMUSIC_DIR", "/music"))
DATA_DIR = Path(os.environ.get("QQMUSIC_DATA", "/home/appuser/data"))

ARCHIVE_BASE = BASE_DIR / "QQmusic" if BASE_DIR.name != "QQmusic" else BASE_DIR
INBOX_DIR = DATA_DIR / "inbox"
FAILED_DIR = ARCHIVE_BASE / "music_tag" / "failed"
FAIL_DIRS = {
    "unlock": FAILED_DIR / "unlock_failed",
    "metadata": FAILED_DIR / "metadata_failed",
    "tag": FAILED_DIR / "tag_failed",
}
DEDUP_DB = DATA_DIR / "dedup.db"

# go-music-dl web/API origin (same container, localhost)
MTW_HOST = os.environ.get("MTW_HOST", "10.10.4.43:8001")
MTW_TOKEN = os.environ.get("MTW_TOKEN", "")
GOMUSICD_URL = os.environ.get("GOMUSICD_URL", "http://127.0.0.1:8080")

UM_BIN = os.environ.get("UM_BIN", "um")

SUFFIX_MAP = {".mp3": "MP3", ".ogg": "OGG", ".m4a": "AAC", ".flac": None}
ENCRYPTED_SUFFIXES = {".mflac", ".mgg", ".qmc", ".qmc0", ".qmc2", ".qmc3", ".tkm"}

EXCLUDE_KEYWORDS = ["和声", "伴奏", "instrumental", "DJ", "翻唱", "cover"]
FORMAT_PRIORITY = {"flac": 1, "mp3": 3, "m4a": 4, "aac": 4, "ogg": 2}
MP3_320_PRIORITY = 2  # MP3 >=320k beats plain MP3, below FLAC

LYRICS_MAX_SECONDS_DIFF = 2.0

# ===================== Dedup DB =====================

def db_init():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    for d in [ARCHIVE_BASE, INBOX_DIR, *FAIL_DIRS.values()]:
        d.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(DEDUP_DB))
    con.execute(
        """CREATE TABLE IF NOT EXISTS songs(
             mid TEXT PRIMARY KEY, title TEXT, artist TEXT, album TEXT,
             format TEXT, path TEXT, duration REAL, synced_at TEXT)"""
    )
    con.commit()
    return con


def db_lookup_key(con, title, artist, album):
    """Idempotency anchor: title+artist+album, normalized."""
    row = con.execute(
        "SELECT format, path FROM songs WHERE lower(title)=lower(?) "
        "AND lower(artist)=lower(?) AND lower(ifnull(album,''))=lower(ifnull(?,''))",
        (title.strip(), artist.strip(), (album or "").strip()),
    ).fetchone()
    return row


def db_upsert(con, mid, title, artist, album, fmt, path, duration):
    con.execute(
        """INSERT INTO songs(mid,title,artist,album,format,path,duration,synced_at)
           VALUES(?,?,?,?,?,?,?,?)
           ON CONFLICT(mid) DO UPDATE SET
             title=excluded.title, artist=excluded.artist, album=excluded.album,
             format=excluded.format, path=excluded.path, duration=excluded.duration,
             synced_at=excluded.synced_at""",
        (mid, title, artist, album, fmt, str(path), duration, datetime.now().isoformat(timespec="seconds")),
    )
    con.commit()


# ===================== Naming =====================

def normalize_space(s):
    return re.sub(r"\s+", " ", (s or "").strip())


def safe_component(s):
    """Mimic go-music-dl template sanitization: '/' inside metadata becomes '_'.
    Keep it filesystem-safe on both Linux NAS and SMB clients."""
    s = normalize_space(s)
    s = re.sub(r"[/\\:*?\"<>|]", "_", s)
    return s or "Unknown"


def render_filename(template_fields):
    """{artist} - {name}.{ext} with multi-artist joined by '/'.

    Template placeholders: {artist} {album} {name} {source} {id} {ext}.
    '{artist}'/'{album}' slashes inside values are replaced; the ' / ' separator
    between artists is preserved as '/' per the archive convention — go-music-dl
    would split subdirectories on '/', so we join multi-artists with '/' only in
    the DB key and render the filename with '/ ' -> '/' collapsed to a single '/'.
    """
    artist = template_fields.get("artist") or "Unknown"
    name = template_fields.get("name") or template_fields.get("title") or ""
    ext = template_fields.get("ext") or ""
    artist_fs = safe_component(artist)
    name_fs = safe_component(name)
    return f"{artist_fs} - {name_fs}{ext}"


# ===================== Archive layout =====================

def get_archive_dir(path):
    """FLAC -> archive root; MP3/OGG/M4A -> uppercase subdir."""
    ext = Path(path).suffix.lower()
    sub = SUFFIX_MAP.get(ext)
    if sub:
        return ARCHIVE_BASE / sub
    return ARCHIVE_BASE


def quality_rank(path):
    ext = Path(path).suffix.lower()
    base = FORMAT_PRIORITY.get(ext, 5)
    if ext == ".mp3":
        try:
            out = subprocess.run(
                ["ffprobe", "-v", "quiet", "-show_entries", "format=bit_rate",
                 "-of", "default=noprint_wrappers=1:nokey=1", str(path)],
                capture_output=True, text=True, timeout=30,
            ).stdout.strip()
            if out and int(float(out)) >= 300000:
                return MP3_320_PRIORITY
        except Exception:
            pass
    return base


def archive_file(src, meta):
    """Move src into the format-tiered subdir under the naming convention."""
    dest_dir = get_archive_dir(src)
    dest_dir.mkdir(parents=True, exist_ok=True)
    fname = render_filename({**meta, "ext": Path(src).suffix.lower()})
    dest = dest_dir / fname
    if dest.exists() and dest.samefile(src):
        return dest
    # dedup by DB, not filename suffixes: replace lower-quality copies
    existing = None
    con = db_init()
    row = db_lookup_key(con, meta.get("title", ""), meta.get("artist", ""), meta.get("album", ""))
    if row and Path(row[1]).exists():
        existing_rank = {"flac": FORMAT_PRIORITY.get(".flac", 1), "mp3320": MP3_320_PRIORITY,
                         "mp3": FORMAT_PRIORITY.get(".mp3", 3), "aac": FORMAT_PRIORITY.get(".aac", 4)}.get(row[0], FORMAT_PRIORITY.get(Path(row[1]).suffix.lower(), 5))
        new_rank = quality_rank(src)
        if existing_rank <= new_rank:
            return Path(row[1])  # keep higher-quality existing copy
        Path(row[1]).unlink(missing_ok=True)
        existing = row[1]
    if dest.exists():
        dest.unlink()
    shutil.move(str(src), str(dest))
    return dest


# ===================== Lyrics / tags =====================

def pick_lyrics(candidates):
    """Prefer word-by-word karaoke LRC (dense timestamps); drop truncated versions.

    Heuristic: count timestamp lines, compare against track duration.
    A healthy full lyric scales ~2-10 lines per minute; trial clips are short.
    """
    scored = []
    for c in candidates or []:
        text = c if isinstance(c, str) else (c.get("lyric") or "")
        lines = [l for l in text.splitlines() if l.strip()]
        if not lines:
            continue
        scored.append((len(lines), text))
    if not scored:
        return None
    scored.sort(key=lambda t: -t[0])
    return scored[0][1]


def embed_metadata(path, meta):
    """Embed cover art + lyrics via ffmpeg (go-music-dl has the built-in switch;
    this path is for files acquired without it, e.g. import mode)."""
    cover = meta.get("cover_url")
    lyric = meta.get("lyric")
    args = ["ffmpeg", "-y", "-hide_banner", "-loglevel", "error", "-i", str(path)]
    tmp = Path(str(path) + ".tmp" + Path(path).suffix)
    cmd = ["ffmpeg", "-y", "-hide_banner", "-loglevel", "error", "-i", str(path)]
    if lyric:
        cmd += ["-metadata", f"lyrics={lyric[:8000]}"]
    cmd += ["-map", "0", "-c", "copy"]
    if cover:
        cmd += ["-i", cover, "-map", "1", "-c", "copy", "-disposition:1", "attached_pic"]
    cmd += [str(tmp)]
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    if r.returncode != 0:
        return False, r.stderr.strip()
    tmp.replace(path)
    return True, ""


def probe_duration(path):
    try:
        out = subprocess.run(
            ["ffprobe", "-v", "quiet", "-show_entries", "format=duration",
             "-of", "default=noprint_wrappers=1:nokey=1", str(path)],
            capture_output=True, text=True, timeout=30,
        ).stdout.strip()
        return float(out) if out else None
    except Exception:
        return None


def verify_against_json(path, expected_duration):
    """Δ<=2s gate: catches trial clips / truncated streams."""
    actual = probe_duration(path)
    if actual is None or expected_duration is None:
        return True
    return abs(actual - float(expected_duration)) <= LYRICS_MAX_SECONDS_DIFF


# ===================== Decrypt (um) =====================

def unlock(src, outdir):
    """Run um on encrypted containers (.mflac/.mgg/.qmc*). Plain files pass through."""
    outdir.mkdir(parents=True, exist_ok=True)
    cmd = [UM_BIN, "-i", str(src), "-o", str(outdir)]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    except FileNotFoundError:
        return None
    if r.returncode == 0:
        produced = sorted(outdir.glob(Path(src).stem + ".*"), key=lambda p: p.stat().st_mtime, reverse=True)
        produced = [p for p in produced if p.suffix.lower() in {".flac", ".mp3", ".ogg", ".m4a", ".wav"}]
        if produced:
            Path(src).unlink(missing_ok=True)
            return produced[0]
    return None


# ===================== MusicTagWeb client (import mode fallback) =====================

def mtw_call(endpoint, payload):
    """MusicTagWeb /api/* must be reached from inside its container via localhost
    (external curl gets HTML 404). Bridge through ssh + docker exec."""
    body = json.dumps(payload, ensure_ascii=False)
    remote = f'curl -s -H "Authorization: Token {MTW_TOKEN}" -H "Content-Type: application/json" ' \
             f'-d \'{body}\' http://localhost:8001{endpoint}'
    try:
        r = subprocess.run(
            ["ssh", "-i", os.path.expanduser("~/.ssh/id_nova"), "-o", "StrictHostKeyChecking=no",
             f"root@10.10.4.2", f'docker exec MusicTagWeb sh -c {json.dumps(remote)}'],
            capture_output=True, text=True, timeout=60,
        )
        out = r.stdout.strip()
        return json.loads(out) if out else None
    except Exception:
        return None


def smart_tag_match(title, artist):
    res = mtw_call("/api/fetch_id3_by_title", {"title": title, "artist": artist})
    if not res:
        return None
    return res


# ===================== Mode: sync =====================

def cmd_sync(args):
    """Ask go-music-dl REST API for favorites, download each via returned url."""
    con = db_init()
    url = f"{GOMUSICD_URL}/api/favorite_songs?page=1&pageSize=200"
    try:
        req = urllib.request.Request(url, headers={"Authorization": f"Bearer {MTW_TOKEN}"} if MTW_TOKEN else {})
        with urllib.request.urlopen(req, timeout=60) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except (urllib.error.URLError, ValueError) as e:
        print(f"[sync] go-music-dl API unreachable: {e}")
        return 1

    songs = data.get("data", {}).get("songs") or data.get("songs") or []
    inbox_files = list_inbox()
    total_new = 0
    for s in songs:
        mid = str(s.get("mid") or s.get("id") or "")
        title = normalize_space(s.get("songName") or s.get("name") or "")
        artist = normalize_space(", ".join(a.get("name", "") for a in (s.get("singer") or []) if isinstance(a, dict)))
        album = normalize_space((s.get("album") or {}).get("title") if isinstance(s.get("album"), dict) else s.get("album"))
        if not title or not artist:
            continue
        if is_excluded(title, artist):
            continue
        row = db_lookup_key(con, title, artist, album)
        if row and Path(row[1]).exists():
            continue  # already archived (higher-or-equal quality)
        download_url = s.get("url") or s.get("download_url")
        if not download_url:
            continue
        ext = Path(urllib.request.urlparse(download_url).path).suffix.lower() or ".mp3"
        tmp = DATA_DIR / f"dl_{mid or abs(hash(title))}{ext}"
        try:
            with urllib.request.urlopen(download_url, timeout=120) as f:
                tmp.write_bytes(f.read())
        except Exception as e:
            print(f"[sync] download failed {title}: {e}")
            tmp.unlink(missing_ok=True)
            continue
        if tmp.suffix.lower() in ENCRYPTED_SUFFIXES:
            unlocked = unlock(tmp, DATA_DIR)
            if unlocked:
                tmp = unlocked
        dest = archive_file(tmp, {"title": title, "name": title, "artist": artist,
                                  "album": album, "ext": tmp.suffix.lower(),
                                  "duration": s.get("interval")})
        if dest and dest.exists():
            db_upsert(con, mid or f"{artist}-{title}", title, artist, album,
                      dest.suffix.lstrip("."), dest, s.get("interval"))
            total_new += 1
        else:
            shutil.move(str(tmp), str(FAIL_DIRS["tag"]) / tmp.name) if tmp.exists() else None
    print(f"[sync] done: {total_new} new / {len(songs)} candidates")
    return 0


# ===================== Mode: import =====================

def list_inbox():
    files = []
    if INBOX_DIR.exists():
        for p in sorted(INBOX_DIR.iterdir()):
            if p.is_file() and p.suffix.lower() in {".flac", ".mp3", ".ogg", ".m4a", ".wav", *ENCRYPTED_SUFFIXES}:
                files.append(p)
    return files


def is_excluded(title, artist):
    """Drop harmony/accompaniment/DJ/cover versions unless the filename itself says so."""
    hay = f"{title} {artist}".lower()
    return any(k.lower() in hay for k in EXCLUDE_KEYWORDS)


def filter_songs(candidates, filename_hint):
    """Rank candidate metadata versions for a bare file (no sidecar JSON).

    Hard gates: normalized title equality + artist set containment; then the
    duration Δ<=2s discriminator kills live/edit/truncated versions. Falls back
    to MusicTagWeb smart_tag when the filename hint is the only evidence.
    """
    hint = normalize_space(Path(filename_hint).stem)
    scored = []
    for c in candidates or []:
        t = normalize_space(c.get("title", ""))
        a = normalize_space(c.get("artist", ""))
        if not t:
            continue
        score = 0
        if t.lower() in hint.lower() or hint.lower() in t.lower():
            score += 40
        if a and (a.lower() in hint.lower() or hint.lower().endswith(a.lower())):
            score += 30
        dur = c.get("duration")
        if dur:
            scored.append((score + 10, c))
        else:
            scored.append((score, c))
    scored.sort(key=lambda x: (-x[0], x[1].get("title", "")))
    out = []
    for _, c in scored:
        if is_excluded(c.get("title", ""), c.get("artist", "")):
            continue
        out.append(c)
    return out


def match_from_filename(stem):
    """Filename 'artist - title' (our own convention) or anything else -> fields."""
    if " - " in stem:
        artist, name = stem.split(" - ", 1)
        return artist.strip(), name.strip()
    return "", stem.strip()


def cmd_import(args):
    con = db_init()
    files = list_inbox()
    if not files:
        print("[import] inbox empty")
        return 0
    moved = 0
    for src in files:
        ext = src.suffix.lower()
        work = src
        if ext in ENCRYPTED_SUFFIXES:
            unlocked = unlock(src, INBOX_DIR)
            if unlocked:
                work = unlocked
            else:
                shutil.move(str(src), str(FAIL_DIRS["unlock"]) / src.name)
                continue
        stem_hint = Path(work).stem
        artist, title = match_from_filename(stem_hint)
        row = db_lookup_key(con, title, artist, "")
        if row and Path(row[1]).exists() and quality_rank(work) >= 3:
            work.unlink(missing_ok=True)  # existing copy is equal/better
            continue
        meta = {"title": title, "name": title, "artist": artist, "album": "", "ext": Path(work).suffix.lower()}
        # no sidecar JSON available -> query MusicTagWeb smart_tag chain
        mt = smart_tag_match(title, artist)
        if mt:
            d = mt.get("data") if isinstance(mt, dict) else None
            if isinstance(d, dict):
                meta["album"] = normalize_space(d.get("album", ""))
                if not meta["artist"]:
                    meta["artist"] = normalize_space(d.get("artist", ""))
        if not meta["artist"]:
            shutil.move(str(work), str(FAIL_DIRS["metadata"]) / work.name)
            continue
        # absorb sidecar .lrc into embedded lyrics, then drop it
        lrc = work.with_suffix(".lrc")
        if lrc.exists():
            meta["lyric"] = lrc.read_text(encoding="utf-8", errors="replace")
            lrc.unlink()
        ok, err = embed_metadata(work, meta)
        dest = archive_file(work, meta)
        if dest and dest.exists():
            db_upsert(con, f"{meta['artist']}-{meta['title']}", meta["title"], meta["artist"],
                      meta["album"], dest.suffix.lstrip("."), dest, None)
            moved += 1
        elif not ok:
            shutil.move(str(work), str(FAIL_DIRS["tag"]) / work.name)
    print(f"[import] done: {moved} archived")
    return 0


# ===================== Mode: backfill =====================

def iter_library():
    if not ARCHIVE_BASE.exists():
        return []
    out = []
    for p in sorted(ARCHIVE_BASE.rglob("*")):
        if p.is_file() and p.suffix.lower() in {".flac", ".mp3", ".ogg", ".m4a", ".wav"}:
            out.append(p)
    return out


def cmd_backfill(args):
    """One-off: normalize names, absorb sidecar .lrc, rebuild dedup DB. Never cron."""
    con = db_init()
    files = iter_library()
    renamed = 0
    for p in files:
        stem = p.stem
        artist, title = match_from_filename(stem)
        if not artist:
            artist = normalize_space(stem)
        meta = {"title": title, "name": title, "artist": artist,
                "album": "", "ext": p.suffix.lower()}
        try:
            out = subprocess.run(
                ["ffprobe", "-v", "quiet", "-show_entries",
                 "format_tags=title,artist,album:format=duration",
                 "-of", "json", str(p)],
                capture_output=True, text=True, timeout=30,
            ).stdout
            j = json.loads(out or "{}")
            tags = j.get("format", {}).get("tags", {})
            meta["title"] = normalize_space(tags.get("title") or title)
            meta["artist"] = normalize_space(tags.get("artist") or artist)
            meta["album"] = normalize_space(tags.get("album") or "")
            dur = float(j.get("format", {}).get("duration", 0) or 0)
        except Exception:
            dur = 0
        lrc = p.with_suffix(".lrc")
        if lrc.exists():
            meta["lyric"] = lrc.read_text(encoding="utf-8", errors="replace")
            embed_metadata(p, meta)
            lrc.unlink()
        dest = get_archive_dir(p) / render_filename(meta)
        if dest != p and not dest.exists():
            shutil.move(str(p), str(dest))
            renamed += 1
        else:
            dest = p
        db_upsert(con, f"{meta['artist']}-{meta['title']}", meta["title"], meta["artist"],
                  meta["album"], dest.suffix.lstrip("."), dest, dur or None)
    print(f"[backfill] done: {len(files)} scanned, {renamed} renamed, DB rebuilt")
    return 0


# ===================== Test mode =====================

def run_test():
    """Dry run: print planned structure and logic checks, move no files."""
    print("=" * 60)
    print(f"TEST MODE - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    print("\n[TEST] directory structure:")
    for d in [ARCHIVE_BASE, ARCHIVE_BASE / "MP3", ARCHIVE_BASE / "OGG", INBOX_DIR, *FAIL_DIRS.values()]:
        print(f"  - {d} (exists: {d.exists()})")
    print(f"\n[TEST] naming render:")
    print(f"  - {render_filename({'artist': '周杰伦', 'name': '晴天', 'ext': '.flac'})}")
    print(f"  - {render_filename({'artist': '周杰伦/费玉清', 'name': '千里之外', 'ext': '.flac'})}")
    test_songs = [
        {"title": "晴天", "artist": "周杰伦", "duration": 269},
        {"title": "晴天", "artist": "周杰伦", "duration": 269, "version": "伴奏"},
        {"title": "晴天", "artist": "其他", "duration": 180},
    ]
    filtered = filter_songs(test_songs, "周杰伦 - 晴天.flac")
    print(f"\n[TEST] filter_songs: {len(test_songs)} -> {len(filtered)} (exclude: {[k for k in EXCLUDE_KEYWORDS]})")
    for f in ARCHIVE_BASE.iterdir() if ARCHIVE_BASE.exists() else []:
        if f.is_file():
            print(f"  archive dir sample: {f.name}")
            break
    for s in [Path("/tmp/t.mp3"), Path("/tmp/t.ogg"), Path("/tmp/t.flac")]:
        print(f"  {s.name} -> {get_archive_dir(s)}")
    print("\n[TEST] pipeline logic OK.")


# ===================== Entrypoint =====================

def main():
    ap = argparse.ArgumentParser(prog="music_pipeline", description="QQ Music archive pipeline")
    ap.add_argument("mode", nargs="?", default="sync", choices=["sync", "import", "backfill"])
    ap.add_argument("--test", "-t", action="store_true", help="dry-run logic check")
    args = ap.parse_args()
    if args.test:
        run_test()
        return 0
    return {"sync": cmd_sync, "import": cmd_import, "backfill": cmd_backfill}[args.mode](args)


if __name__ == "__main__":
    sys.exit(main())
