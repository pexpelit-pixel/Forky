import argparse
import asyncio
import json
import logging
import os
import re
import random
import sys
import time
from pathlib import Path
from typing import Optional, Tuple, List
from urllib.parse import urlparse

import aiofiles
import aiohttp
from bs4 import BeautifulSoup
from tqdm import tqdm


# ---------- Fungsi Validasi URL ----------
def is_valid_url(url: str) -> bool:
    """Memvalidasi apakah string merupakan URL yang valid."""
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except ValueError:
        return False


def is_doodstream_url(url: str) -> bool:
    """Memvalidasi apakah URL mengarah ke format DoodStream yang diharapkan."""
    if not is_valid_url(url):
        return False
    return "/e/" in url or "/d/" in url


def sanitize_filename(name: str, max_length: int = 120) -> str:
    """Membersihkan string agar aman dipakai sebagai nama file."""
    name = re.sub(r'[\\/*?:"<>|]', "", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name[:max_length] if len(name) > max_length else name


# ---------- Setup Logger ----------
def setup_logger(level=logging.INFO):
    log_format = logging.Formatter(
        "%(asctime)s [%(levelname)s] - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    root_logger = logging.getLogger()
    if root_logger.hasHandlers():
        root_logger.handlers.clear()
    root_logger.setLevel(level)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(log_format)
    root_logger.addHandler(console_handler)


# ---------- API DoodStream ----------
class DoodStreamAPI:
    """Kelas untuk berinteraksi dengan DoodStream API."""

    def __init__(self, session: aiohttp.ClientSession):
        self.session = session
        self.logger = logging.getLogger(__name__)

    async def get_download_url(self, url: str) -> Optional[Tuple[str, str]]:
        self.logger.info(f"Memproses URL: {url}")
        embed_url = url.replace('/d/', '/e/')
        try:
            self.session.headers.update({"Referer": embed_url})
            async with self.session.get(embed_url) as response:
                response.raise_for_status()
                html_content = await response.text()

            pass_md5_match = re.search(r'/pass_md5/([^"\']+)', html_content)
            if not pass_md5_match:
                self.logger.error("Tidak dapat menemukan 'pass_md5' pada halaman embed.")
                return None

            pass_md5_path = pass_md5_match.group(1)
            domain = urlparse(embed_url).netloc
            pass_md5_url = f"https://{domain}/pass_md5/{pass_md5_path}"
            self.logger.debug(f"URL pass_md5 ditemukan: {pass_md5_url}")

            async with self.session.get(pass_md5_url) as md5_response:
                md5_response.raise_for_status()
                media_url_base = await md5_response.text()

            token = pass_md5_path.split('/')[-1]
            random_chars = ''.join(
                random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789', k=10)
            )
            final_url = f"{media_url_base}{random_chars}?token={token}&expiry={int(time.time())}"

            soup = BeautifulSoup(html_content, "html.parser")
            title_tag = soup.find("title")
            title = title_tag.text.strip() if title_tag else token
            title = sanitize_filename(title)

            self.logger.info(f"Direct download link berhasil dibuat untuk '{title}'")
            return final_url, title
        except aiohttp.ClientError as e:
            self.logger.error(f"Request error saat mengakses DoodStream: {e}")
        except Exception as e:
            self.logger.error(f"Terjadi kesalahan saat memproses URL DoodStream: {e}", exc_info=True)
        return None


# ---------- Downloader Utama (dengan dukungan .part) ----------
class DoodStreamFastDL:
    """Mengunduh video dari DoodStream dengan dukungan resume dan file sementara."""

    def __init__(self, url: str, output_path: str, show_progress: bool = True):
        self.url = url
        self.output_path = output_path          # final .mp4
        self.temp_path = output_path + ".part"  # file sementara
        self.show_progress = show_progress
        self.logger = logging.getLogger(__name__)

    async def download(self) -> str:
        """Mengunduh video, mengembalikan path final jika sukses, string kosong jika gagal."""
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
        }
        async with aiohttp.ClientSession(headers=headers) as session:
            api = DoodStreamAPI(session)
            result = await api.get_download_url(self.url)
            if not result:
                self.logger.error("Gagal mendapatkan informasi video.")
                return ""

            direct_url, title = result
            self.logger.info(f"Video akan disimpan di: {os.path.abspath(self.output_path)}")
            success = await self._download_file(session, direct_url)
            return self.output_path if success else ""

    async def _download_file(self, session: aiohttp.ClientSession, url: str) -> bool:
        """Download dengan resume menggunakan file .part, return True jika sukses."""
        try:
            # Cek ukuran file sementara
            existing_size = 0
            if os.path.exists(self.temp_path):
                existing_size = os.path.getsize(self.temp_path)
                self.logger.info(f"File sementara ditemukan ({existing_size} bytes), melanjutkan unduhan...")
            else:
                self.logger.info("Memulai unduhan baru...")

            headers = {}
            if existing_size > 0:
                headers["Range"] = f"bytes={existing_size}-"

            async with session.get(url, headers=headers, timeout=None) as response:
                # Tangani kasus server tidak mendukung resume
                if existing_size > 0 and response.status == 200:
                    self.logger.warning("Server tidak mendukung resume, memulai dari awal.")
                    if os.path.exists(self.temp_path):
                        os.remove(self.temp_path)
                    existing_size = 0
                    # Mulai ulang request tanpa header Range
                    async with session.get(url, timeout=None) as fresh_response:
                        fresh_response.raise_for_status()
                        return await self._write_stream(fresh_response, 0)
                elif response.status == 416:
                    # Range tidak valid, mungkin file sudah lengkap
                    self.logger.info("File sudah lengkap (HTTP 416). Mengganti nama menjadi final.")
                    os.rename(self.temp_path, self.output_path)
                    return True

                response.raise_for_status()

                total_size = None
                if "Content-Range" in response.headers:
                    total_size = int(response.headers["Content-Range"].split("/")[-1])
                elif "Content-Length" in response.headers:
                    total_size = existing_size + int(response.headers["Content-Length"])

                # Cek jika file final sudah ada dengan ukuran total yang diketahui
                if total_size and os.path.exists(self.output_path) and os.path.getsize(self.output_path) == total_size:
                    self.logger.info("File final sudah lengkap, melewati.")
                    if os.path.exists(self.temp_path):
                        os.remove(self.temp_path)
                    return True

                return await self._write_stream(response, existing_size, total_size)

        except aiohttp.ClientError as e:
            self.logger.error(f"Gagal mengunduh: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Error tidak terduga: {e}")
            return False

    async def _write_stream(self, response, initial_size: int, total_size: Optional[int] = None) -> bool:
        """Menulis stream response ke file .part, lalu rename ke final."""
        mode = "ab" if initial_size > 0 else "wb"
        progress_bar = None
        if self.show_progress and total_size:
            progress_bar = tqdm(
                total=total_size,
                initial=initial_size,
                unit="B",
                unit_scale=True,
                unit_divisor=1024,
                desc=os.path.basename(self.output_path)
            )

        try:
            async with aiofiles.open(self.temp_path, mode) as f:
                chunk_size = 8192
                async for chunk in response.content.iter_chunked(chunk_size):
                    await f.write(chunk)
                    if progress_bar:
                        progress_bar.update(len(chunk))

            if progress_bar:
                progress_bar.close()

            # Rename .part menjadi final
            if os.path.exists(self.output_path):
                os.remove(self.output_path)  # timpa jika ada
            os.rename(self.temp_path, self.output_path)
            self.logger.info("Unduhan selesai.")
            return True
        except Exception as e:
            if progress_bar:
                progress_bar.close()
            self.logger.error(f"Gagal menulis file: {e}")
            return False


# ---------- Upload ke API Videy ----------
async def upload_to_api(session: aiohttp.ClientSession, file_path: str, title: str) -> Optional[str]:
    """Unggah file video ke API Videy, mengembalikan link video atau None jika gagal."""
    api_url = "https://videy.sakittakberdarah.workers.dev/api/upload"
    logger = logging.getLogger("Uploader")

    try:
        data = aiohttp.FormData()
        data.add_field("title", title)

        with open(file_path, "rb") as fp:
            data.add_field(
                "file",
                fp,
                filename=os.path.basename(file_path),
                content_type="video/mp4"
            )
            logger.info(f"Mengunggah {file_path} ke Videy...")
            async with session.post(api_url, data=data) as resp:
                resp_text = await resp.text()
                if resp.status == 200:
                    logger.info(f"Upload sukses. Respon: {resp_text}")
                    # Coba ekstrak link dari JSON
                    try:
                        resp_json = json.loads(resp_text)
                        link = resp_json.get("url") or resp_json.get("link")
                        if link:
                            return link
                        else:
                            logger.warning("Tidak ada field 'url' atau 'link', mengembalikan teks mentah.")
                            return resp_text.strip()
                    except json.JSONDecodeError:
                        logger.warning("Respon bukan JSON, mengembalikan teks mentah.")
                        return resp_text.strip()
                else:
                    logger.error(f"Upload gagal (HTTP {resp.status}): {resp_text}")
                    return None
    except Exception as e:
        logger.error(f"Exception saat upload: {e}")
        return None


# ---------- Fungsi untuk memproses satu video (download) ----------
async def download_single_video(
    idx: int,
    video: dict,
    output_dir: Path,
    semaphore: asyncio.Semaphore,
    show_progress: bool,
    max_retries: int = 3
) -> Optional[str]:
    """Download satu video, return path final jika berhasil, None jika gagal."""
    logger = logging.getLogger(f"Video#{idx}")
    async with semaphore:
        if not isinstance(video, dict):
            logger.warning("Data tidak valid, dilewati.")
            return None

        iframe_src = video.get("iframe_src")
        original_title = video.get("title", "Tanpa Judul")

        if not iframe_src:
            logger.warning("Tidak memiliki iframe_src, dilewati.")
            return None
        if not is_doodstream_url(iframe_src):
            logger.warning(f"iframe_src tidak valid: {iframe_src}")
            return None

        safe_part = sanitize_filename(original_title[:50])
        local_filename = f"{idx:03d}_{safe_part}.mp4"
        local_path = output_dir / local_filename

        # Cek jika file final sudah ada -> lewati
        if local_path.exists():
            logger.info(f"File {local_path.name} sudah ada, melewati download.")
            return str(local_path)

        # Coba download dengan retry
        for attempt in range(1, max_retries + 1):
            logger.info(f"Percobaan download ke-{attempt} untuk {local_filename}")
            downloader = DoodStreamFastDL(
                url=iframe_src,
                output_path=str(local_path),
                show_progress=show_progress
            )
            result_path = await downloader.download()
            if result_path and os.path.exists(result_path):
                logger.info(f"Download sukses: {result_path}")
                return result_path
            else:
                logger.warning(f"Percobaan {attempt} gagal.")
                if attempt < max_retries:
                    wait = 5 * attempt
                    logger.info(f"Menunggu {wait} detik sebelum mencoba lagi...")
                    await asyncio.sleep(wait)

        logger.error(f"Semua percobaan download gagal untuk video #{idx}.")
        return None


# ---------- CLI Entry Point (dioptimalkan) ----------
async def main():
    parser = argparse.ArgumentParser(
        description="DoodStreamFastDL + Upload ke Videy (dioptimalkan)",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog="Contoh:\n"
               "  python script.py -j videos.json\n"
               "  python script.py -j videos.json -o ./downloads --keep-local --concurrency 3"
    )
    parser.add_argument("-j", "--json", default="bokepi_videos.json",
                        help="Path ke file JSON yang berisi daftar video (default: all_videos.json).")
    parser.add_argument("-o", "--output", dest="output_dir", type=str, default=".",
                        help="Direktori penyimpanan video sementara (default: current dir).")
    parser.add_argument("--no-progress", action="store_true",
                        help="Sembunyikan progress bar download.")
    parser.add_argument("--keep-local", action="store_true",
                        help="Simpan file lokal setelah upload (default: hapus setelah upload).")
    parser.add_argument("--concurrency", type=int, default=3,
                        help="Jumlah download paralel (default: 3).")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="Log verbose.")
    args = parser.parse_args()

    log_level = logging.DEBUG if args.verbose else logging.INFO
    setup_logger(log_level)

    # Baca JSON
    try:
        with open(args.json, "r", encoding="utf-8") as f:
            videos = json.load(f)
    except Exception as e:
        logging.error(f"Gagal membaca file JSON: {e}")
        return

    if not isinstance(videos, list):
        logging.error("Format JSON harus berupa array.")
        return

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Tahap 1: Download semua video
    logging.info("=== TAHAP DOWNLOAD ===")
    semaphore = asyncio.Semaphore(args.concurrency)
    download_tasks = []
    for idx, video in enumerate(videos, start=1):
        task = download_single_video(
            idx, video, output_dir, semaphore,
            show_progress=not args.no_progress
        )
        download_tasks.append(task)

    # Jalankan semua download secara konkuren
    results = await asyncio.gather(*download_tasks, return_exceptions=True)

    # Kumpulkan path yang berhasil
    downloaded_paths = []
    for res in results:
        if isinstance(res, Exception):
            logging.error(f"Task download menghasilkan exception: {res}")
        elif res is not None:
            downloaded_paths.append(res)

    logging.info(f"Download selesai. {len(downloaded_paths)}/{len(videos)} video berhasil diunduh.")

    if not downloaded_paths:
        logging.warning("Tidak ada video yang berhasil diunduh. Keluar.")
        return

    # Tahap 2: Upload semua video yang berhasil diunduh
    logging.info("=== TAHAP UPLOAD ===")
    link_output_file = Path("link.txt")
    async with aiohttp.ClientSession() as session:
        success_count = 0
        for file_path in downloaded_paths:
            # Ambil nama file tanpa ekstensi untuk digunakan sebagai title upload
            file_name = Path(file_path).stem  # e.g., "001_judul"
            upload_title = file_name
            logging.info(f"Mengupload {file_name}...")

            link = await upload_to_api(session, file_path, upload_title)
            if link:
                # Tulis link ke link.txt segera
                async with aiofiles.open(link_output_file, "a") as lf:
                    await lf.write(link + "\n")
                logging.info(f"Link disimpan ke {link_output_file}: {link}")
                success_count += 1
                # Hapus file lokal jika tidak disimpan
                if not args.keep_local:
                    try:
                        os.remove(file_path)
                        logging.info(f"File lokal '{file_path}' dihapus.")
                    except OSError as e:
                        logging.warning(f"Gagal menghapus file: {e}")
            else:
                logging.error(f"Upload gagal untuk {file_name}")

    logging.info(f"Proses selesai. {success_count}/{len(downloaded_paths)} video berhasil diupload.")
    if link_output_file.exists():
        logging.info(f"Semua link tersimpan di {link_output_file.resolve()}")


if __name__ == "__main__":
    asyncio.run(main())
