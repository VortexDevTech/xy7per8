import asyncio
import os
import sys
import subprocess
from pathlib import Path
import traceback
from typing import List, Optional, Dict, Tuple
import json
import math
import re
import base64
import zipfile
import shutil
from concurrent.futures import ProcessPoolExecutor, as_completed

import yt_dlp
from yt_dlp.networking.impersonate import ImpersonateTarget
import httpx
from bs4 import BeautifulSoup
from pyrogram import Client
from pyrogram.errors import FloodWait, RPCError
from pyrogram.types import InputMediaPhoto


# ============================================================================
# ENVIRONMENT DETECTION
# ============================================================================

def get_environment() -> str:
    if "GITHUB_ACTIONS" in os.environ:
        return "github_actions"
    elif "google.colab" in sys.modules:
        return "colab"
    return "unknown"


ENVIRONMENT = get_environment()

SERVER_PRIORITY = ["donghuastream", "animecube", "lucifer", "xiao", "sea", "comixy"]
ALLOWED_SERVERS = set(SERVER_PRIORITY)


# ============================================================================
# CONFIGURATION
# ============================================================================

class Config:

    @staticmethod
    def get_secret(key: str, default=None):
        if ENVIRONMENT == "colab":
            try:
                from google.colab import userdata
                value = userdata.get(key)
                if value is not None and value != "":
                    return value
            except Exception:
                pass

        value = os.environ.get(key)
        if value is None or value == "":
            return default
        return value

    DB_PATH = "./db.json"
    TEMP_DIR = "./temp"
    DOWNLOAD_DIR = "./downloads"

    TELEGRAM_API_ID = int(get_secret("TELEGRAM_API_ID", 26684954))
    TELEGRAM_API_HASH = get_secret(
        "TELEGRAM_API_HASH", "a709a6225180f08416b5d9effd1c9fd1"
    )
    TELEGRAM_USER_SESSION = get_secret("TG_SESSION_STRING")
    TELEGRAM_BOT_TOKEN = get_secret("TELEGRAM_BOT_TOKEN")
    TELEGRAM_MAIN_CHANNEL_ID = int(
        get_secret("TELEGRAM_MAIN_CHANNEL_ID", -1003028652784)
    )
    TELEGRAM_FORWARDED_CHANNEL_ID = int(
        get_secret("TELEGRAM_FORWARDED_CHANNEL_ID", -1003794526596)
    )
    TELEGRAM_MAIN_CHANNEL = "Donghua_Sigmas"
    MAX_FILE_SIZE = 1.85 * 1024 * 1024 * 1024


# ============================================================================
# DATABASE MANAGER
# ============================================================================

class DatabaseManager:

    @staticmethod
    def load() -> List[Dict]:
        if os.path.exists(Config.DB_PATH):
            try:
                with open(Config.DB_PATH, "r", encoding="utf-8") as f:
                    return json.load(f)
            except json.JSONDecodeError:
                print("⚠️ db.json corrupted. Starting fresh.")
        return []

    @staticmethod
    def save(db: List[Dict]):
        with open(Config.DB_PATH, "w", encoding="utf-8") as f:
            json.dump(db, f, indent=4, ensure_ascii=False)

    @staticmethod
    def is_downloaded(db: List[Dict], uid: str) -> bool:
        return any(entry.get("uid") == uid for entry in db)

    @staticmethod
    def add_entry(
        db: List[Dict],
        uid: str,
        episode: str,
        link: str,
        server: str,
        qualities: List[int],
    ):
        for entry in db:
            if entry.get("uid") == uid:
                existing = set(entry.get("qualities_downloaded", []))
                existing.update(qualities)
                entry["qualities_downloaded"] = sorted(existing, reverse=True)
                entry["server"] = server
                entry["link"] = link
                return

        db.append(
            {
                "episode": episode,
                "uid": uid,
                "link": link,
                "server": server,
                "qualities_downloaded": sorted(qualities, reverse=True),
            }
        )


# ============================================================================
# UTILITIES
# ============================================================================

class Utils:

    @staticmethod
    def split_video_if_needed(
        file_path: Path, max_size: int = Config.MAX_FILE_SIZE
    ) -> List[Path]:
        size = file_path.stat().st_size
        if size <= max_size:
            return [file_path]

        print(f"    ✂️ Splitting: {file_path.name}")
        try:
            duration = float(
                subprocess.run(
                    [
                        "ffprobe", "-v", "error",
                        "-show_entries", "format=duration",
                        "-of", "default=noprint_wrappers=1:nokey=1",
                        str(file_path),
                    ],
                    capture_output=True,
                    text=True,
                ).stdout.strip()
            )

            parts = math.ceil(size / max_size)
            segment_time = math.ceil(duration / parts)
            out_pattern = (
                file_path.parent / f"{file_path.stem}_part%03d{file_path.suffix}"
            )

            subprocess.run(
                [
                    "ffmpeg", "-i", str(file_path),
                    "-c", "copy", "-map", "0",
                    "-f", "segment", "-segment_time", str(segment_time),
                    "-reset_timestamps", "1", str(out_pattern),
                ],
                check=True,
            )

            parts_list = sorted(
                file_path.parent.glob(f"{file_path.stem}_part*{file_path.suffix}")
            )
            if parts_list:
                file_path.unlink(missing_ok=True)
            return parts_list

        except Exception as e:
            print(f"    ❌ Split failed: {e}")
            return [file_path]

    @staticmethod
    def get_file_info_from_yt_dlp(url: str, config: Optional[Dict] = None) -> Dict:
        ydl_opts = {
            "quiet": True,
            "no_warnings": True,
            "extract_flat": False,
            "impersonate": ImpersonateTarget.from_str("safari"),
        }
        if config:
            ydl_opts.update(config)

        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            try:
                return ydl.extract_info(url, download=False)
            except yt_dlp.utils.DownloadError as e:
                raise RuntimeError(f"yt-dlp extraction failed: {e}")

    @staticmethod
    def download_cover(url: str, output_dir: str) -> Optional[str]:
        """Download cover image. Returns path or None."""
        if not url:
            return None
        out = Path(output_dir)
        out.mkdir(parents=True, exist_ok=True)
        try:
            with httpx.Client(timeout=60.0, follow_redirects=True) as client:
                r = client.get(url)
                r.raise_for_status()
                path = out / "cover.jpg"
                with open(path, "wb") as f:
                    f.write(r.content)
                return str(path)
        except Exception as e:
            print(f"    ⚠️ Cover download failed: {e}")
            return None

    @staticmethod
    def download_subtitles(
        tracks: List[Dict], output_dir: str
    ) -> List[str]:
        """
        Download subtitle files from unified track list.

        tracks: [{"url": "...", "label": "English", "code": "en"}, ...]
        Returns list of downloaded file paths.
        """
        if not tracks:
            return []

        out_dir = Path(output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        downloaded = []

        with httpx.Client(timeout=60.0, follow_redirects=True) as client:
            for track in tracks:
                try:
                    url = track["url"]
                    label = track.get("label", "unknown")
                    code = track.get("code", "und")

                    # Determine extension from URL
                    url_ext = os.path.splitext(url.split("?")[0])[1].lower()
                    if url_ext not in (".vtt", ".srt", ".ass", ".ssa"):
                        url_ext = f".{track.get('ext', 'vtt')}"

                    # Build filename
                    if label and label != code:
                        filename = f"{label}_{code}{url_ext}"
                    else:
                        filename = f"{code}{url_ext}"

                    path = out_dir / filename

                    r = client.get(url)
                    r.raise_for_status()

                    with open(path, "wb") as f:
                        f.write(r.content)

                    downloaded.append(str(path))

                except Exception as e:
                    print(f"    ⚠️ Subtitle download failed ({track.get('code', '?')}): {e}")

        return downloaded

    @staticmethod
    def extract_subtitle_tracks_from_ytdlp(info_dict: Dict) -> List[Dict]:
        """Extract subtitle tracks from yt-dlp info_dict into unified format."""
        tracks = []

        for code, subs_list in info_dict.get("subtitles", {}).items():
            # Prefer vtt > srt > ass > first available
            best = None
            for sub in subs_list:
                ext = sub.get("ext", "")
                if ext == "vtt":
                    best = sub
                    break
                elif ext in ("srt", "ass", "ssa") and best is None:
                    best = sub
                elif best is None:
                    best = sub

            if best and best.get("url"):
                tracks.append(
                    {
                        "url": best["url"],
                        "label": code,
                        "code": code,
                        "ext": best.get("ext", "vtt"),
                    }
                )

        return tracks

    @staticmethod
    def get_standard_quality_by_width(formats: List[Dict]) -> List[Dict]:
        if not formats:
            return []

        def width_to_res(width):
            if width >= 3800:
                return 2160
            if width >= 2500:
                return 1440
            if width >= 1900:
                return 1080
            if width >= 1200:
                return 720
            if width >= 800:
                return 480
            return 360

        video_formats = [
            f for f in formats if f.get("vcodec") != "none" and f.get("width")
        ]

        res_map: Dict[int, Dict] = {}
        for f in video_formats:
            res = width_to_res(f["width"])
            if res not in res_map or f["width"] > res_map[res]["width"]:
                f["quality"] = res
                res_map[res] = f

        selected = []
        for res in [2160, 1080, 720]:
            if res in res_map:
                selected.append(res_map[res])
        if 480 in res_map:
            selected.append(res_map[480])
        elif 360 in res_map:
            selected.append(res_map[360])

        return selected

    @staticmethod
    def generate_screenshots(
        video_path: Path, output_dir: Path, count: int = 5
    ) -> List[str]:
        """Generate evenly-spaced screenshots from a video file."""
        try:
            result = subprocess.run(
                [
                    "ffprobe", "-v", "error",
                    "-show_entries", "format=duration",
                    "-of", "default=noprint_wrappers=1:nokey=1",
                    str(video_path),
                ],
                capture_output=True,
                text=True,
            )
            duration = float(result.stdout.strip())

            if duration <= 0:
                return []

            screenshots = []
            interval = duration / (count + 1)

            for i in range(1, count + 1):
                ts = interval * i
                out_path = output_dir / f"screenshot_{i:02d}.jpg"

                subprocess.run(
                    [
                        "ffmpeg", "-y",
                        "-ss", f"{ts:.2f}",
                        "-i", str(video_path),
                        "-vframes", "1",
                        "-q:v", "2",
                        str(out_path),
                    ],
                    check=True,
                    capture_output=True,
                )

                if out_path.exists() and out_path.stat().st_size > 0:
                    screenshots.append(str(out_path))

            print(f"  📸 Generated {len(screenshots)} screenshot(s)")
            return screenshots

        except Exception as e:
            print(f"  ⚠️ Screenshot generation failed: {e}")
            return []

    @staticmethod
    def cleanup():
        if os.path.exists(Config.TEMP_DIR):
            shutil.rmtree(Config.TEMP_DIR)
        os.makedirs(Config.TEMP_DIR, exist_ok=True)

    @staticmethod
    def cleanup_episode_dir(ep_dir: str):
        if ep_dir and os.path.exists(ep_dir):
            shutil.rmtree(ep_dir)
            print(f"  🗑️ Freed disk: {ep_dir}")


# ============================================================================
# VIDEO PROCESSOR
# ============================================================================

class VideoProcessor:

    @staticmethod
    def process_format(
        format_data: Dict,
        sub_files: List[str],
        ep_dir: str,
        anime_title: str,
        ep_number: str,
    ) -> Tuple[bool, int]:
        quality = format_data.get("quality", 0)
        stream_url = format_data.get("url")

        if not stream_url:
            return False, quality

        final = os.path.join(
            ep_dir,
            f"[{ep_number}] [{anime_title}] [{quality}p] "
            f"[@{Config.TELEGRAM_MAIN_CHANNEL}].mkv",
        )

        if os.path.exists(final):
            print(f"    ⏭️ {quality}p already exists")
            return True, quality

        print(f"    🎬 Downloading {quality}p ...")

        try:
            subprocess.run(
                [
                    "yt-dlp", "-N", "16",
                    "--impersonate", "safari",
                    "-f", "bv+ba/b",
                    "--merge-output-format", "mkv",
                    stream_url, "-o", final,
                    "--quiet", "--no-warnings",
                ],
                check=True,
            )

            # Mux subtitles into MKV
            if sub_files:
                temp_out = final.replace(".mkv", "_muxed.mkv")
                cmd = ["ffmpeg", "-y", "-i", final]

                for sub in sub_files:
                    cmd.extend(["-i", sub])

                cmd.extend(["-map", "0:v", "-map", "0:a?"])

                for i in range(len(sub_files)):
                    cmd.extend(["-map", f"{i + 1}:0"])

                cmd.extend(["-c:v", "copy", "-c:a", "copy", "-c:s", "srt"])

                # Add language metadata per subtitle track
                for i, sub in enumerate(sub_files):
                    stem = Path(sub).stem
                    if "_" in stem:
                        label, code = stem.rsplit("_", 1)
                    else:
                        label = code = stem

                    cmd.extend(
                        [
                            f"-metadata:s:s:{i}",
                            f"language={code}",
                            f"-metadata:s:s:{i}",
                            f"title={label}",
                        ]
                    )

                cmd.append(temp_out)

                subprocess.run(cmd, check=True, capture_output=True, text=True)
                os.replace(temp_out, final)

            print(f"    ✅ {quality}p saved")
            return True, quality

        except subprocess.CalledProcessError as e:
            print(f"    ❌ {quality}p failed (exit {e.returncode})")
            if e.stderr:
                print(f"       {e.stderr.strip()[:300]}")
            # Clean partial file
            try:
                Path(final).unlink(missing_ok=True)
            except Exception:
                pass
            return False, quality


# ============================================================================
# ANOBOYE SCRAPER
# ============================================================================

class AnoBoye:

    def __init__(self):
        self.base_url = "https://anoboye.com"
        self.headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 safari/537.36"
            ),
            "Accept": (
                "text/html,application/xhtml+xml,"
                "application/xml;q=0.9,*/*;q=0.8"
            ),
        }

    # ------------------------------------------------------------------
    def get_latest_episodes(self, endpoint: str = "/") -> List[Dict]:
        url = self.base_url + endpoint

        with httpx.Client(
            headers=self.headers, timeout=60.0, follow_redirects=True
        ) as client:
            response = client.get(url)
            response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")
        results = []

        for article in soup.find_all("div", class_="bsx"):
            a_tag = article.find("a", itemprop="url")
            if not a_tag:
                continue

            href = a_tag.get("href", "")

            h2 = article.find("h2", itemprop="headline")
            name = a_tag.get("title") or (h2.text.strip() if h2 else "Unknown")
            name = re.sub(r'[\\/*?:"<>|]', "", name)

            img_tag = article.find("img", itemprop="image")
            image = (
                (img_tag.get("data-src") or img_tag.get("src"))
                if img_tag
                else None
            )

            type_tag = article.find("div", class_="typez")
            anime_type = type_tag.text.strip() if type_tag else "Unknown"

            ep_tag = article.find("span", class_="epx")
            ep_raw = ep_tag.text.strip() if ep_tag else "00"
            ep_number = re.sub(r"[^\d.]", "", ep_raw) or ep_raw

            results.append(
                {
                    "name": name,
                    "type": anime_type,
                    "ep_number": ep_number,
                    "href": href,
                    "image": image,
                    "uid": f"{name}_Ep{ep_number}".replace(" ", "_"),
                }
            )

        return results

    # ------------------------------------------------------------------
    def extract_all_players(self, endpoint: str) -> Dict[str, Dict[str, str]]:
        """
        Returns structured map of every allowed player.

        Comixy Sub is treated as darkplayer.

        Example:
        {
            "donghuastream": {"darkplayer": "abc123", "dailyplayer": "kXYZ789"},
            "comixy":        {"darkplayer": "def456"},
        }
        """
        with httpx.Client(
            headers=self.headers, timeout=15.0, follow_redirects=True
        ) as client:
            response = client.get(endpoint)
            response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")
        players: Dict[str, Dict[str, str]] = {}

        for card in soup.find_all("button", class_="server-card"):
            hostname = card.get("data-hostname", "").strip()
            raw_b64 = card.get("data-value", "")
            if not raw_b64 or not hostname:
                continue

            hostname_lower = hostname.lower()

            # Identify server
            server = None
            for s in ALLOWED_SERVERS:
                if s in hostname_lower:
                    server = s
                    break
            if server is None:
                print(f"    ⏭️ Ignoring server: {hostname}")
                continue

            # Identify player type
            if "dark" in hostname_lower:
                ptype = "darkplayer"
            elif "daily" in hostname_lower:
                ptype = "dailyplayer"
            elif "sub" in hostname_lower:
                ptype = "darkplayer"  # Comixy Sub == DarkPlayer
            else:
                ptype = "darkplayer"

            # Decode base64
            padded = raw_b64 + "=" * (-len(raw_b64) % 4)
            try:
                decoded = base64.b64decode(padded).decode(
                    "utf-8", errors="ignore"
                )
            except Exception:
                continue

            # Extract ID
            player_id = None

            if ptype == "darkplayer":
                m = re.search(r"id=([a-zA-Z0-9]+)", decoded)
                if m:
                    player_id = m.group(1)
                else:
                    m = re.search(r'src=["\']([^"\']+)["\']', decoded)
                    if m:
                        player_id = m.group(1)
                    else:
                        m = re.search(r"https?://[^\s\"'<>]+", decoded)
                        if m:
                            player_id = m.group(0)

            elif ptype == "dailyplayer":
                m = re.search(r"id=(k[a-zA-Z0-9]+)", decoded)
                if m:
                    player_id = m.group(1)

            if player_id:
                players.setdefault(server, {})[ptype] = player_id
                tag = (
                    f"{player_id[:35]}..."
                    if len(player_id) > 35
                    else player_id
                )
                print(f"    🎮 {hostname} → {ptype} ({tag})")

        return players

    # ------------------------------------------------------------------
    def extract_darkplayer_config(self, player_id: str) -> Optional[Dict]:
        """
        Request darkplayer.php?id=<id>, parse HTML,
        extract config (videoUrl, tracks, thumbnail).

        Returns:
        {
            "video_url": "https://...",
            "tracks": [{"url": "...", "label": "English", "code": "en"}, ...],
            "thumbnail": "https://...",
        }
        """
        if player_id.startswith("http"):
            # Already a full URL, can't extract config page
            return None

        url = f"{self.base_url}/watch/darkplayer.php?id={player_id}"

        try:
            with httpx.Client(
                headers=self.headers, timeout=15.0, follow_redirects=True
            ) as client:
                response = client.get(url)
                response.raise_for_status()

            html = response.text

            # Extract videoUrl
            m = re.search(
                r'videoUrl\s*:\s*"((?:[^"\\]|\\.)*)"', html
            )
            video_url = (
                m.group(1).replace("\\/", "/") if m else None
            )

            # Extract tracks JSON array
            tracks = []
            m = re.search(r"tracks\s*:\s*(\[.*?\])", html, re.DOTALL)
            if m:
                try:
                    raw_tracks = json.loads(m.group(1))
                    for t in raw_tracks:
                        file_url = t.get("file", "").replace("\\/", "/")
                        if file_url:
                            tracks.append(
                                {
                                    "url": file_url,
                                    "label": t.get("label", "Unknown"),
                                    "code": t.get("code", "und"),
                                }
                            )
                except json.JSONDecodeError:
                    pass

            # Extract thumbnail
            m = re.search(
                r'thumbnail\s*:\s*"((?:[^"\\]|\\.)*)"', html
            )
            thumbnail = (
                m.group(1).replace("\\/", "/") if m else None
            )

            if video_url:
                print(
                    f"    📋 DarkPlayer config: "
                    f"url=✓ tracks={len(tracks)} thumb={'✓' if thumbnail else '✗'}"
                )
                return {
                    "video_url": video_url,
                    "tracks": tracks,
                    "thumbnail": thumbnail,
                }

            return None

        except Exception as e:
            print(f"    ⚠️ DarkPlayer config extraction failed: {e}")
            return None

    # ------------------------------------------------------------------
    def darkplayer_manifest_url(self, player_id: str) -> str:
        """Fallback: build manifest URL directly."""
        if player_id.startswith("http"):
            return player_id
        return (
            f"{self.base_url}/watch/darkplayer.php?"
            f"action=playlist&id={player_id}"
        )


# ============================================================================
# TELEGRAM UPLOADER
# ============================================================================

class TelegramUploader:

    @staticmethod
    async def _progress(current: int, total: int, pbar):
        pbar.n = current
        pbar.update(0)

    @staticmethod
    async def upload_episode(
        ep_dir: str,
        user_client: Client,
        bot_client: Client,
    ) -> List[int]:
        """
        Upload all MKV files, subtitles.zip, and screenshots.
        Returns ALL sent message IDs for forwarding.
        """
        if ENVIRONMENT == "colab":
            from tqdm.notebook import tqdm
        else:
            from tqdm import tqdm

        ep_path = Path(ep_dir)
        cover = ep_path / "cover.jpg"
        thumb = str(cover) if cover.exists() else None
        zip_file = ep_path / "subtitles.zip"

        sent_ids: List[int] = []

        # ---- build video queue (split if needed) ----
        video_queue: List[Path] = []
        for mkv in sorted(ep_path.glob("*.mkv")):
            video_queue.extend(
                Utils.split_video_if_needed(mkv, Config.MAX_FILE_SIZE)
            )

        # ---- upload videos ----
        if video_queue:
            largest = max(video_queue, key=lambda p: p.stat().st_size)
            semaphore = asyncio.Semaphore(3)

            async def worker(
                fp: Path, client: Client, pos: int
            ) -> Optional[int]:
                async with semaphore:
                    size = fp.stat().st_size
                    pbar = tqdm(
                        total=size,
                        unit="B",
                        unit_scale=True,
                        unit_divisor=1024,
                        desc=f"📤 {fp.name[:30]}",
                        position=pos,
                        leave=True,
                    )
                    try:
                        msg = await client.send_document(
                            chat_id=Config.TELEGRAM_MAIN_CHANNEL_ID,
                            document=str(fp),
                            thumb=thumb,
                            caption=f"`{fp.name}`",
                            progress=TelegramUploader._progress,
                            progress_args=(pbar,),
                        )
                        return msg.id if msg else None

                    except FloodWait as e:
                        print(f"\n  ⏳ FloodWait {e.value}s")
                        await asyncio.sleep(e.value)
                        msg = await client.send_document(
                            chat_id=Config.TELEGRAM_MAIN_CHANNEL_ID,
                            document=str(fp),
                            thumb=thumb,
                            caption=f"`{fp.name}`",
                        )
                        return msg.id if msg else None

                    except RPCError as e:
                        print(f"\n  ❌ Upload error: {e}")
                        return None

                    finally:
                        pbar.close()

            print(f"  🚀 Uploading {len(video_queue)} video file(s)...")

            tasks = [
                worker(
                    v,
                    user_client if v == largest else bot_client,
                    i,
                )
                for i, v in enumerate(video_queue)
            ]
            results = await asyncio.gather(*tasks)
            sent_ids.extend(mid for mid in results if mid is not None)

        # ---- upload subtitles zip ----
        if zip_file.exists():
            if ENVIRONMENT == "colab":
                from tqdm.notebook import tqdm
            else:
                from tqdm import tqdm

            print("  📦 Uploading subtitles.zip")
            pbar = tqdm(
                total=zip_file.stat().st_size,
                unit="B",
                unit_scale=True,
                desc="📤 subtitles.zip",
            )
            try:
                msg = await bot_client.send_document(
                    chat_id=Config.TELEGRAM_MAIN_CHANNEL_ID,
                    document=str(zip_file),
                    caption="Subtitles",
                    progress=TelegramUploader._progress,
                    progress_args=(pbar,),
                )
                if msg:
                    sent_ids.append(msg.id)
            except RPCError as e:
                print(f"  ❌ Subtitle upload error: {e}")
            finally:
                pbar.close()

        # ---- upload screenshots as media group ----
        screenshots = sorted(ep_path.glob("screenshot_*.jpg"))
        if screenshots:
            print(f"  📸 Uploading {len(screenshots)} screenshot(s)...")
            try:
                media = [
                    InputMediaPhoto(str(s))
                    for s in screenshots
                ]

                messages = await bot_client.send_media_group(
                    chat_id=Config.TELEGRAM_MAIN_CHANNEL_ID,
                    media=media,
                )
                if messages:
                    sent_ids.extend(m.id for m in messages)
                    print(
                        f"  ✅ Screenshots sent "
                        f"({len(messages)} message(s))"
                    )

            except FloodWait as e:
                print(f"  ⏳ Screenshot flood-wait {e.value}s")
                await asyncio.sleep(e.value)
                messages = await bot_client.send_media_group(
                    chat_id=Config.TELEGRAM_MAIN_CHANNEL_ID,
                    media=[InputMediaPhoto(str(s)) for s in screenshots],
                )
                if messages:
                    sent_ids.extend(m.id for m in messages)

            except RPCError as e:
                print(f"  ❌ Screenshot upload error: {e}")

        if not sent_ids:
            print("  ⚠️ Nothing was uploaded.")
        else:
            print(f"  ✅ Upload done — {len(sent_ids)} message(s) total")

        return sent_ids

    # ------------------------------------------------------------------
    @staticmethod
    async def forward_messages(
        client: Client,
        message_ids: List[int],
        from_chat: int,
        to_chat: int,
    ):
        """Forward ALL uploaded messages to the forwarded channel."""
        if not message_ids:
            return
        if from_chat == to_chat:
            print("  ℹ️ Forward skipped (same channel)")
            return

        try:
            await client.forward_messages(
                chat_id=to_chat,
                from_chat_id=from_chat,
                message_ids=message_ids,
            )
            print(f"  📨 Forwarded {len(message_ids)} message(s)")

        except FloodWait as e:
            print(f"  ⏳ Forward flood-wait {e.value}s")
            await asyncio.sleep(e.value)
            await client.forward_messages(
                chat_id=to_chat,
                from_chat_id=from_chat,
                message_ids=message_ids,
            )
            print("  📨 Forwarded after wait")

        except RPCError as e:
            print(f"  ❌ Forward failed: {e}")


# ============================================================================
# EPISODE PROCESSOR
# ============================================================================

class EpisodeProcessor:

    def __init__(self, anoboye: AnoBoye):
        self.anoboye = anoboye

    # ------------------------------------------------------------------
    def _select_best_source(
        self, players: Dict[str, Dict[str, str]]
    ) -> Optional[Dict]:
        """
        Walk SERVER_PRIORITY. For each server:

        1. DarkPlayer → extract config → probe with yt-dlp
           - Has 4K → USE IT
           - No 4K → try DailyPlayer
        2. DailyPlayer → probe with yt-dlp
           - Has formats → USE IT
        3. DarkPlayer had formats but no 4K → USE IT (fallback)
        4. Nothing → next server

        Returns dict:
        {
            "info_dict": ...,
            "formats": [...],
            "source_label": "donghuastream darkplayer",
            "subtitle_tracks": [{"url":..., "label":..., "code":...}],
            "thumbnail_url": "https://..." or None,
        }
        """

        for server in SERVER_PRIORITY:
            if server not in players:
                continue

            sp = players[server]
            print(f"  🔍 Checking {server.title()} ...")

            dark_info = None
            dark_fmts: List[Dict] = []
            dark_sub_tracks: List[Dict] = []
            dark_thumbnail: Optional[str] = None

            # ---- probe darkplayer ----
            if "darkplayer" in sp:
                try:
                    # Step 1: Extract config from darkplayer page
                    config = self.anoboye.extract_darkplayer_config(
                        sp["darkplayer"]
                    )

                    if config and config.get("video_url"):
                        video_url = config["video_url"]
                        dark_sub_tracks = config.get("tracks", [])
                        dark_thumbnail = config.get("thumbnail")
                    else:
                        # Fallback to direct manifest URL
                        video_url = self.anoboye.darkplayer_manifest_url(
                            sp["darkplayer"]
                        )
                        dark_sub_tracks = []
                        dark_thumbnail = None

                    # Step 2: Probe with yt-dlp
                    dark_info = Utils.get_file_info_from_yt_dlp(video_url)
                    dark_fmts = Utils.get_standard_quality_by_width(
                        dark_info.get("formats", [])
                    )

                    has_4k = any(
                        f.get("quality") == 2160 for f in dark_fmts
                    )

                    if has_4k:
                        label = f"{server} darkplayer"
                        print(
                            f"  ✅ {server.title()} DarkPlayer "
                            f"has 4K — selected"
                        )
                        return {
                            "info_dict": dark_info,
                            "formats": dark_fmts,
                            "source_label": label,
                            "subtitle_tracks": dark_sub_tracks,
                            "thumbnail_url": dark_thumbnail,
                        }

                    if dark_fmts:
                        qs = ", ".join(
                            str(f["quality"]) for f in dark_fmts
                        )
                        print(
                            f"  ⚠️ {server.title()} DarkPlayer "
                            f"no 4K (has [{qs}]p), "
                            f"trying DailyPlayer..."
                        )
                    else:
                        print(
                            f"  ⚠️ {server.title()} DarkPlayer — "
                            f"no usable formats, trying DailyPlayer..."
                        )

                except Exception as e:
                    print(
                        f"  ❌ {server.title()} DarkPlayer "
                        f"probe failed: {e}"
                    )

            # ---- probe dailyplayer ----
            if "dailyplayer" in sp:
                try:
                    dm_url = (
                        f"https://www.dailymotion.com/video/"
                        f"{sp['dailyplayer']}"
                    )
                    daily_info = Utils.get_file_info_from_yt_dlp(dm_url)
                    daily_fmts = Utils.get_standard_quality_by_width(
                        daily_info.get("formats", [])
                    )

                    if daily_fmts:
                        label = f"{server} dailyplayer"
                        daily_sub_tracks = (
                            Utils.extract_subtitle_tracks_from_ytdlp(
                                daily_info
                            )
                        )
                        print(
                            f"  ✅ Using {server.title()} DailyPlayer"
                        )
                        return {
                            "info_dict": daily_info,
                            "formats": daily_fmts,
                            "source_label": label,
                            "subtitle_tracks": daily_sub_tracks,
                            "thumbnail_url": daily_info.get("thumbnail"),
                        }

                    print(
                        f"  ⚠️ {server.title()} DailyPlayer — "
                        f"no usable formats"
                    )

                except Exception as e:
                    print(
                        f"  ❌ {server.title()} DailyPlayer "
                        f"probe failed: {e}"
                    )

            # ---- fallback to darkplayer without 4K ----
            if dark_fmts and dark_info:
                label = f"{server} darkplayer"
                print(
                    f"  ✅ Falling back to {server.title()} "
                    f"DarkPlayer (no 4K)"
                )
                return {
                    "info_dict": dark_info,
                    "formats": dark_fmts,
                    "source_label": label,
                    "subtitle_tracks": dark_sub_tracks,
                    "thumbnail_url": dark_thumbnail,
                }

            print(
                f"  ⏭️ {server.title()} exhausted, "
                f"trying next server..."
            )

        return None

    # ------------------------------------------------------------------
    def process_episode(
        self, ep: Dict, db: List[Dict]
    ) -> Optional[Tuple[str, List[int], str]]:
        """
        Full pipeline for one episode.
        Returns (ep_dir, qualities_downloaded, server_used) or None.
        """
        uid = ep["uid"]

        if DatabaseManager.is_downloaded(db, uid):
            print(f"  ⏭️ Already in DB — skipping")
            return None

        print(f"\n{'=' * 60}")
        print(f"🚀 {ep['name']}  |  Episode {ep['ep_number']}")
        print(f"{'=' * 60}")

        ep_dir: Optional[str] = None

        try:
            # ---- extract all players ----
            print("  📡 Extracting players ...")
            players = self.anoboye.extract_all_players(ep["href"])

            if not players:
                print("  ❌ No allowed players found — skipping")
                return None

            # ---- select best source ----
            result = self._select_best_source(players)

            if result is None:
                print(
                    "  ❌ No usable source from allowed servers "
                    "— skipping"
                )
                return None

            info_dict = result["info_dict"]
            target_formats = result["formats"]
            source_label = result["source_label"]
            subtitle_tracks = result["subtitle_tracks"]
            thumbnail_url = result["thumbnail_url"]

            print(f"  📺 Source: {source_label}")

            # ---- prepare directory ----
            clean_title = ep["name"].replace(" ", "_")
            ep_dir = os.path.join(
                Config.DOWNLOAD_DIR,
                f"{clean_title}_Ep_{ep['ep_number']}",
            )
            os.makedirs(ep_dir, exist_ok=True)

            # ---- cover image ----
            cover_url = ep.get("image") or thumbnail_url
            if cover_url:
                Utils.download_cover(cover_url, ep_dir)
                print("  ✅ Cover downloaded")

            # ---- download subtitles ----
            subtitles = Utils.download_subtitles(
                subtitle_tracks, ep_dir
            )
            if subtitles:
                print(
                    f"  ✅ {len(subtitles)} subtitle file(s) downloaded"
                )
            else:
                print("  ℹ️ No subtitles available")

            # ---- download all formats in parallel ----
            qlabels = ", ".join(
                f"{f['quality']}p" for f in target_formats
            )
            print(f"  🎯 Formats: [{qlabels}]")

            success_qualities: List[int] = []

            with ProcessPoolExecutor(max_workers=4) as pool:
                futures = {
                    pool.submit(
                        VideoProcessor.process_format,
                        fmt,
                        subtitles,
                        ep_dir,
                        clean_title,
                        ep["ep_number"],
                    ): fmt.get("quality")
                    for fmt in target_formats
                }

                for fut in as_completed(futures):
                    ok, quality = fut.result()
                    if ok:
                        success_qualities.append(quality)

            # ---- zip subtitles ----
            if subtitles:
                zip_path = os.path.join(ep_dir, "subtitles.zip")
                print(f"  📦 Zipping {len(subtitles)} subtitle(s)")
                with zipfile.ZipFile(
                    zip_path, "w", zipfile.ZIP_DEFLATED
                ) as zf:
                    for sub in subtitles:
                        zf.write(sub, arcname=os.path.basename(sub))
                for sub in subtitles:
                    try:
                        os.remove(sub)
                    except FileNotFoundError:
                        pass

            # ---- generate screenshots from smallest video ----
            if success_qualities:
                mkvs = sorted(
                    Path(ep_dir).glob("*.mkv"),
                    key=lambda p: p.stat().st_size,
                )
                if mkvs:
                    smallest = mkvs[0]
                    print(
                        f"  📸 Generating screenshots from: "
                        f"{smallest.name}"
                    )
                    Utils.generate_screenshots(
                        smallest, Path(ep_dir), count=5
                    )

            # ---- result ----
            if success_qualities:
                print(
                    f"  ✅ Downloaded: "
                    f"{sorted(success_qualities, reverse=True)}"
                )
                return ep_dir, success_qualities, source_label

            print("  ❌ All format downloads failed")
            Utils.cleanup_episode_dir(ep_dir)
            return None

        except Exception as e:
            print(
                f"  ❌ Pipeline error: "
                f"{e.__class__.__name__}: {e}"
            )
            traceback.print_exc()
            if ep_dir:
                Utils.cleanup_episode_dir(ep_dir)
            return None


# ============================================================================
# MAIN
# ============================================================================

async def main():

    user_client = Client(
        "user_session",
        api_id=Config.TELEGRAM_API_ID,
        api_hash=Config.TELEGRAM_API_HASH,
        session_string=Config.TELEGRAM_USER_SESSION,
        workers=6,
        in_memory=True,
        max_concurrent_transmissions=6,
    )

    bot_client = Client(
        "bot_session",
        api_id=Config.TELEGRAM_API_ID,
        api_hash=Config.TELEGRAM_API_HASH,
        bot_token=Config.TELEGRAM_BOT_TOKEN,
        workers=6,
        in_memory=True,
        max_concurrent_transmissions=6,
    )

    try:
        await user_client.start()
        await bot_client.start()
        print("✅ Telegram clients started\n")

        anoboye = AnoBoye()
        db = DatabaseManager.load()
        processor = EpisodeProcessor(anoboye)

        all_episodes = [anoboye.get_latest_episodes()[1]]
        print(f"📊 Found {len(all_episodes)} episode(s)\n")

        for idx, ep in enumerate(all_episodes, 1):
            print(
                f"\n[{idx}/{len(all_episodes)}] "
                f"{ep['name']} — Ep {ep['ep_number']}"
            )

            ep_dir: Optional[str] = None

            try:
                result = processor.process_episode(ep, db)

                if result is None:
                    continue

                ep_dir, qualities, server_used = result

                # ---- upload everything ----
                message_ids = await TelegramUploader.upload_episode(
                    ep_dir, user_client, bot_client
                )

                # ---- forward ALL messages ----
                if message_ids:
                    await TelegramUploader.forward_messages(
                        bot_client,
                        message_ids,
                        Config.TELEGRAM_MAIN_CHANNEL_ID,
                        Config.TELEGRAM_FORWARDED_CHANNEL_ID,
                    )

                # ---- update database ----
                DatabaseManager.add_entry(
                    db,
                    uid=ep["uid"],
                    episode=ep["name"],
                    link=ep["href"],
                    server=server_used,
                    qualities=qualities,
                )
                DatabaseManager.save(db)
                print(f"  💾 Database updated")

            except Exception as e:
                print(f"  ❌ Outer error: {e}")
                traceback.print_exc()

            finally:
                if ep_dir:
                    Utils.cleanup_episode_dir(ep_dir)
                Utils.cleanup()

            await asyncio.sleep(2)

        print(f"\n{'=' * 60}")
        print("✅ All episodes processed")
        print(f"{'=' * 60}")

    except Exception as e:
        print(f"❌ Fatal: {e}")
        traceback.print_exc()

    finally:
        for c in (user_client, bot_client):
            try:
                await c.stop()
            except Exception:
                pass
        print("🛑 Clients stopped")


# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    if ENVIRONMENT == "colab":
        import nest_asyncio

        nest_asyncio.apply()

    asyncio.run(main())
