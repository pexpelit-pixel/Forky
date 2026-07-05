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
from typing import Optional, Tuple
from urllib.parse import urlparse

import aiofiles
import aiohttp
from bs4 import BeautifulSoup
from tqdm.asyncio import tqdm


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


# ---------- Downloader Utama ----------
class DoodStreamFastDL:
    """Mengunduh video dari DoodStream dengan dukungan resume."""

    def __init__(self, url: str, output_path: Optional[str] = None, show_progress: bool = True):
        self.url = url
        self.output_path = output_path
        self.show_progress = show_progress
        self.logger = logging.getLogger(__name__)

    async def download(self) -> str:
        """Mengunduh video dan mengembalikan path file hasil unduhan."""
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

            # Tentukan path penyimpanan
            if self.output_path:
                if os.path.isdir(self.output_path):
                    final_path = os.path.join(self.output_path, f"{title}.mp4")
                else:
                    final_path = self.output_path
            else:
                final_path = f"{title}.mp4"

            self.logger.info(f"Video akan disimpan di: {os.path.abspath(final_path)}")
            await self._download_file(session, direct_url, final_path)
            return final_path

    async def _download_file(self, session: aiohttp.ClientSession, url: str, path: str) -> None:
        """Mengunduh file dari URL dengan dukungan resume otomatis."""
        try:
            existing_size = 0
            if os.path.exists(path):
                existing_size = os.path.getsize(path)
                self.logger.info(f"File sudah ada ({existing_size} bytes). Melanjutkan unduhan...")
            else:
                self.logger.info("Memulai unduhan baru...")

            headers = {}
            if existing_size > 0:
                headers["Range"] = f"bytes={existing_size}-"

            async with session.get(url, headers=headers, timeout=None) as response:
                if existing_size > 0 and response.status == 200:
                    self.logger.warning("Server tidak mendukung resume, mengunduh ulang dari awal.")
                    await self._restart_download(session, url, path)
                    return

                if response.status == 416:
                    self.logger.info("File sudah lengkap, tidak perlu mengunduh lagi.")
                    return

                response.raise_for_status()

                total_size = None
                if "Content-Range" in response.headers:
                    total_size = int(response.headers["Content-Range"].split("/")[-1])
                elif "Content-Length" in response.headers:
                    total_size = existing_size + int(response.headers["Content-Length"])

                if total_size and existing_size >= total_size:
                    self.logger.info("File sudah lengkap, tidak perlu mengunduh lagi.")
                    return

                self.logger.info("Memulai proses pengunduhan...")

                mode = "ab" if existing_size > 0 else "wb"
                progress_bar = None
                if self.show_progress and total_size:
                    progress_bar = tqdm(
                        total=total_size,
                        initial=existing_size,
                        unit="B",
                        unit_scale=True,
                        unit_divisor=1024,
                        desc=os.path.basename(path)
                    )

                async with aiofiles.open(path, mode) as f:
                    chunk_size = 8192
                    async for chunk in response.content.iter_chunked(chunk_size):
                        await f.write(chunk)
                        if progress_bar:
                            progress_bar.update(len(chunk))

                if progress_bar:
                    progress_bar.close()

                self.logger.info("Unduhan selesai.")
        except aiohttp.ClientError as e:
            self.logger.error(f"Gagal mengunduh file: {e}")
        except Exception as e:
            self.logger.error(f"Terjadi kesalahan saat menyimpan file: {e}")

    async def _restart_download(self, session: aiohttp.ClientSession, url: str, path: str) -> None:
        """Menghapus file yang ada dan memulai ulang unduhan dari awal."""
        try:
            os.remove(path)
            self.logger.info("File yang ada dihapus, memulai unduhan baru.")
        except OSError:
            pass
        await self._download_file(session, url, path)


# ---------- Upload ke API Videy ----------
async def upload_to_api(file_path: str, title: str) -> bool:
    """Unggah file video ke API Videy dengan title tertentu."""
    api_url = "https://videy.sakittakberdarah.workers.dev/api/upload"
    logger = logging.getLogger("Uploader")

    async with aiohttp.ClientSession() as session:
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
                        logger.info(f"Upload berhasil! Respon: {resp_text}")
                        return True
                    else:
                        logger.error(f"Upload gagal (HTTP {resp.status}): {resp_text}")
                        return False
        except Exception as e:
            logger.error(f"Exception saat upload: {e}")
            return False


# ---------- CLI Entry Point (modifikasi) ----------
async def main():
    parser = argparse.ArgumentParser(
        description="DoodStreamFastDL + Upload ke Videy",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog="Contoh:\n"
               "  python script.py -j videos.json\n"
               "  python script.py -j videos.json -o ./downloads --keep-local"
    )
    parser.add_argument("-j", "--json", required=True,
                        help="Path ke file JSON yang berisi daftar video (array of {title, iframe_src}).")
    parser.add_argument("-o", "--output", dest="output_dir", type=str, default=".",
                        help="Direktori penyimpanan video sementara (default: current dir).")
    parser.add_argument("--no-progress", action="store_true",
                        help="Sembunyikan progress bar download.")
    parser.add_argument("--keep-local", action="store_true",
                        help="Simpan file lokal setelah upload (default: hapus setelah upload).")
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

    success_count = 0
    for idx, video in enumerate(videos, start=1):
        if not isinstance(video, dict):
            logging.warning(f"Data video #{idx} tidak valid, dilewati.")
            continue

        iframe_src = video.get("iframe_src")
        original_title = video.get("title", "Tanpa Judul")

        if not iframe_src:
            logging.warning(f"Video #{idx} tidak memiliki iframe_src, dilewati.")
            continue

        if not is_doodstream_url(iframe_src):
            logging.warning(f"Video #{idx} punya iframe_src tidak valid, dilewati: {iframe_src}")
            continue

        # Buat judul untuk upload: "1 || Chindo Cantik ..."
        upload_title = f"{idx} || {original_title}"

        logging.info(f"=== Memproses #{idx}: {upload_title} ===")

        # Unduh video, nama file unik berdasarkan indeks
        safe_part = sanitize_filename(original_title[:50])  # batasi panjang
        local_filename = f"{idx:03d}_{safe_part}.mp4"
        local_path = output_dir / local_filename

        downloader = DoodStreamFastDL(
            url=iframe_src,
            output_path=str(local_path),
            show_progress=not args.no_progress
        )
        downloaded_path = await downloader.download()

        if not downloaded_path or not os.path.exists(downloaded_path):
            logging.error(f"Download #{idx} gagal, lanjut ke video berikutnya.")
            continue

        # Upload
        upload_ok = await upload_to_api(downloaded_path, upload_title)

        if upload_ok:
            success_count += 1

        # Hapus file lokal jika tidak ingin disimpan
        if not args.keep_local and os.path.exists(downloaded_path):
            os.remove(downloaded_path)
            logging.info(f"File lokal '{downloaded_path}' dihapus.")

        logging.info(f"=== #{idx} selesai ===\n")

    logging.info(f"Proses selesai. {success_count}/{len(videos)} video berhasil diupload.")


if __name__ == "__main__":
    asyncio.run(main())