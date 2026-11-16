"""GridFS та робота з файлами."""
import hashlib
import io
import logging
import os
from datetime import datetime, timezone
from typing import Callable, Dict, Iterator, Optional, Tuple

from bson import ObjectId
from gridfs import GridFS, GridFSBucket
from PIL import Image
from pymongo.database import Database

logger = logging.getLogger(__name__)


# ---------- Управління файлами через GridFS ----------

class FileManager:
    """Робота з файлами в GridFS: зображення, мініатюри, стрімінг"""

    def __init__(self, db: Database) -> None:
        """db — база, у якій GridFS створює fs.files і fs.chunks."""
        self.db = db
        self.fs = GridFS(db)
        self.bucket = GridFSBucket(db)

    def upload_file(self, file_content: bytes, filename: str,
                    metadata: Optional[Dict] = None) -> ObjectId:
        """Завантаження файлу з метаданими"""

        file_hash = hashlib.md5(file_content).hexdigest()

        existing = self.fs.find_one({"metadata.file_hash": file_hash})
        if existing:
            logger.info(f"File already exists: {existing._id}")
            return existing._id

        metadata = dict(metadata or {})
        metadata.update({
            "upload_date": datetime.now(timezone.utc),
            "file_hash": file_hash,
            "file_size": len(file_content)
        })

        file_id = self.fs.put(
            file_content,
            filename=filename,
            metadata=metadata,
            content_type=metadata.get("content_type", "application/octet-stream")
        )

        return file_id

    def upload_image_bytes(self, image_bytes: bytes, filename: str,
                           metadata: Optional[Dict] = None,
                           thumbnail_size: Tuple[int, int] = (200, 200)) -> Dict:
        """Те саме, що upload_image_with_thumbnails, але з байтів (для API-завантажень)."""
        metadata = dict(metadata or {})

        original_id = self.upload_file(image_bytes, filename, metadata)

        pil_image = Image.open(io.BytesIO(image_bytes))
        pil_image.thumbnail(thumbnail_size)

        if pil_image.mode not in ("RGB", "L"):
            pil_image = pil_image.convert("RGB")
        thumbnail_bytes = io.BytesIO()
        pil_image.save(thumbnail_bytes, format="JPEG")

        thumbnail_id = self.upload_file(
            thumbnail_bytes.getvalue(),
            f"thumb_{filename}",
            {**metadata, "is_thumbnail": True, "original_id": original_id,
             "content_type": "image/jpeg"}
        )

        return {
            "original_id": original_id,
            "thumbnail_id": thumbnail_id
        }

    def upload_image_with_thumbnails(self, image_path: str,
                                     metadata: Optional[Dict] = None) -> Dict:
        """Завантаження зображення та створення прев'ю"""

        with open(image_path, "rb") as f:
            image_bytes = f.read()

        return self.upload_image_bytes(image_bytes, os.path.basename(image_path), metadata)

    def get_file_with_streaming(self, file_id: ObjectId, chunk_size: int = 1024 * 1024) -> Dict:
        """Потокове отримання великих файлів"""

        file = self.fs.get(file_id)

        def file_generator() -> Iterator[bytes]:
            """Віддає файл шматками по chunk_size байт."""
            while file.tell() < file.length:
                chunk = file.read(chunk_size)
                if not chunk:
                    break
                yield chunk

        return {
            "filename": file.filename,
            "content_type": (file.metadata or {}).get("content_type") or file.content_type,
            "size": file.length,
            "metadata": file.metadata,
            "stream": file_generator()
        }

    def delete_file_and_thumbnails(self, file_id: ObjectId) -> int:
        """Видалення файлу та пов'язаних з ним файлів. Повертає кількість видалених файлів."""

        self.fs.get(file_id)

        deleted = 0
        for thumb in self.fs.find({"metadata.original_id": file_id}):
            self.fs.delete(thumb._id)
            deleted += 1

        self.fs.delete(file_id)
        deleted += 1

        logger.info(f"Deleted file {file_id} and associated thumbnails")
        return deleted


# ---------- Оптимізація великих файлів ----------

class LargeFileOptimizer:
    """Завантаження великих файлів частинами з контрольною сумою"""

    def __init__(self, db: Database) -> None:
        """db — база для GridFSBucket."""
        self.bucket = GridFSBucket(db)

    def upload_large_file_in_chunks(self, file_path: str,
                                    metadata: Optional[Dict] = None) -> ObjectId:
        """Завантаження великих файлів чанками"""

        upload_stream = self.bucket.open_upload_stream(
            os.path.basename(file_path),
            metadata=metadata,
            chunk_size_bytes=1024 * 1024
        )

        with open(file_path, "rb") as f:
            while True:
                chunk = f.read(1024 * 1024)
                if not chunk:
                    break
                upload_stream.write(chunk)

        upload_stream.close()
        return upload_stream._id

    def download_large_file_with_progress(self, file_id: ObjectId, output_path: str,
                                          on_progress: Optional[Callable[[float], None]] = None
                                          ) -> int:
        """Завантаження з відображенням прогресу. Повертає кількість байтів."""

        file = self.bucket.open_download_stream(file_id)
        total_size = file.length

        downloaded = 0
        with open(output_path, "wb") as f:
            while downloaded < total_size:
                chunk = file.read(1024 * 1024)
                if not chunk:
                    break
                f.write(chunk)
                downloaded += len(chunk)

                progress = (downloaded / total_size) * 100
                logger.info(f"Download progress: {progress:.1f}%")
                if on_progress:
                    on_progress(progress)

        file.close()
        logger.info(f"File downloaded to {output_path}")
        return downloaded
