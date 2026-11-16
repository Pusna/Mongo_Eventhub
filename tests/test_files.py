"""Розділ 10: GridFS — завантаження, мініатюри, стрімінг, великі файли."""
import io
import os
import uuid

import gridfs
import pytest
from PIL import Image

from app.files import FileManager, LargeFileOptimizer


def png_bytes(size=(800, 600), mode="RGBA", color=(255, 0, 0, 128)) -> bytes:
    buf = io.BytesIO()
    Image.new(mode, size, color).save(buf, "PNG")
    return buf.getvalue()


@pytest.fixture
def fm(db):
    return FileManager(db)


def test_upload_file_with_dedup(fm):
    content = f"hello {uuid.uuid4()}".encode()
    meta = {"content_type": "text/plain", "owner": "pytest"}
    first = fm.upload_file(content, "hello.txt", meta)
    assert fm.upload_file(content, "copy.txt") == first
    assert meta == {"content_type": "text/plain", "owner": "pytest"}

    info = fm.fs.get(first)
    assert info.metadata["file_size"] == len(content)
    assert info.metadata["owner"] == "pytest"


def test_image_thumbnails_and_streaming(fm, tmp_path):
    path = tmp_path / f"pic_{uuid.uuid4().hex[:6]}.png"
    path.write_bytes(png_bytes(color=(10, 200, 30, 255)))
    ids = fm.upload_image_with_thumbnails(str(path), {"content_type": "image/png"})

    thumb = fm.get_file_with_streaming(ids["thumbnail_id"], chunk_size=512)
    data = b"".join(thumb["stream"])
    assert Image.open(io.BytesIO(data)).size == (200, 150)
    assert thumb["content_type"] == "image/jpeg"
    assert thumb["metadata"]["original_id"] == ids["original_id"]

    original = fm.get_file_with_streaming(ids["original_id"])
    assert original["content_type"] == "image/png"
    assert b"".join(original["stream"]) == path.read_bytes()


def test_upload_image_without_metadata(fm):
    """У PDF {**metadata} з metadata=None падав з TypeError; RGBA → JPEG теж падав."""
    ids = fm.upload_image_bytes(png_bytes(color=(1, 2, 3, uuid.uuid4().int % 255)), "noalpha.png")
    assert fm.fs.exists(ids["thumbnail_id"])


def test_delete_file_and_thumbnails(fm):
    ids = fm.upload_image_bytes(png_bytes(color=(9, 9, uuid.uuid4().int % 255, 255)), "del.png")
    assert fm.delete_file_and_thumbnails(ids["original_id"]) == 2
    assert not fm.fs.exists(ids["original_id"])
    assert not fm.fs.exists(ids["thumbnail_id"])
    with pytest.raises(gridfs.errors.NoFile):
        fm.delete_file_and_thumbnails(ids["original_id"])


def test_large_file_roundtrip(db, tmp_path):
    lfo = LargeFileOptimizer(db)
    src = tmp_path / "big.bin"
    src.write_bytes(os.urandom(2 * 1024 * 1024 + 7))
    file_id = lfo.upload_large_file_in_chunks(str(src), {"kind": "pytest"})

    assert db.fs.chunks.count_documents({"files_id": file_id}) == 3
    progress = []
    out = tmp_path / "out.bin"
    assert lfo.download_large_file_with_progress(file_id, str(out), progress.append) == src.stat().st_size
    assert out.read_bytes() == src.read_bytes()
    assert progress[-1] == 100
