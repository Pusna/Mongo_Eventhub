"""FastAPI Application - EventHub Pro API."""
import logging
import threading
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from typing import Any, AsyncIterator, Dict, List, Optional

from bson import ObjectId
from bson.errors import InvalidId
from fastapi import BackgroundTasks, Depends, FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from gridfs.errors import NoFile
from pydantic import BaseModel, EmailStr, Field

from app.analytics import AnalyticsAggregator
from app.config import settings
from app.files import FileManager
from app.operations import EventHubOperations
from app.streams import ChangeStreamManager

logger = logging.getLogger(__name__)


# ---------- Залежності ----------

class Services:

    def __init__(self, db_name: Optional[str] = None) -> None:
        """Створює репозиторій і сервіси поверх одного клієнта."""
        self.repo = EventHubOperations(db_name=db_name)
        self.analytics = AnalyticsAggregator(self.repo.db)
        self.files = FileManager(self.repo.db)
        self.streams = ChangeStreamManager(self.repo.client, self.repo.db.name)


@lru_cache
def get_services() -> Services:
    """Залежність FastAPI: один спільний Services на процес"""
    return Services()


def to_json(value: Any) -> Any:
    if isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {k: to_json(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [to_json(v) for v in value]
    return value


def oid(value: str) -> ObjectId:
    """Рядок -> ObjectId; неправильний формат -> HTTP 400."""
    try:
        return ObjectId(value)
    except (InvalidId, TypeError):
        raise HTTPException(status_code=400, detail=f"Invalid id: {value}")


def parse_object_ids(data: Dict, paths: List[str]) -> Dict:
    """Рядкові id з JSON (organizer.user_id тощо) -> ObjectId."""
    for path in paths:
        *parents, leaf = path.split(".")
        node = data
        for p in parents:
            node = node.get(p) if isinstance(node, dict) else None
        if isinstance(node, dict) and isinstance(node.get(leaf), str):
            node[leaf] = oid(node[leaf])
    return data


# ---------- Фоновий Change Stream ----------

_stream_stop = threading.Event()


def _run_change_stream(services: Services) -> None:

    while not _stream_stop.is_set():
        try:
            services.streams.watch_registrations_and_update_cache(
                resume_after=services.streams.load_resume_token("registrations"),
                timeout=30, stop=_stream_stop)
        except Exception as e:
            logger.error(f"Change stream stopped: {e}")
            _stream_stop.wait(5)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Запуск фонових процесів при старті"""
    thread = None
    if settings.api_change_streams:
        _stream_stop.clear()
        thread = threading.Thread(target=_run_change_stream, args=(get_services(),), daemon=True)
        thread.start()
    yield
    _stream_stop.set()
    if thread:
        thread.join(timeout=5)


app = FastAPI(title="EventHub Pro API", version="1.0.0", lifespan=lifespan)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------- Pydantic Models ----------

class UserCreate(BaseModel):
    """Тіло запиту POST /users."""

    email: EmailStr
    profile: Dict
    address: Optional[Dict] = None
    interests: Optional[List[str]] = []


class EventCreate(BaseModel):
    """Тіло запиту POST /events."""

    title: str
    description: str
    category: str
    schedule: Dict
    location: Dict
    capacity: Dict
    organizer: Dict
    tags: Optional[List[str]] = []
    status: Optional[str] = None


class RegistrationCreate(BaseModel):
    """Тіло запиту POST /registrations."""

    user_id: str
    event_id: str


class ReviewCreate(BaseModel):
    """Тіло запиту POST /reviews."""

    user_id: str
    event_id: str
    rating: int = Field(ge=1, le=5)
    comment: str


# ---------- Фонові задачі ----------

def send_confirmation_email(services: Services, registration: RegistrationCreate) -> None:
    """У PDF функція не визначена. Лист ставиться в чергу notifications (outbox)."""
    services.repo.db.notifications.insert_one({
        "user_id": ObjectId(registration.user_id),
        "event_id": ObjectId(registration.event_id),
        "channels": ["email"],
        "template": "registration_confirmation",
        "status": "pending",
        "created_at": datetime.now(timezone.utc),
    })


# ---------- API Endpoints ----------

@app.get("/")
def root() -> Dict[str, str]:
    """Назва та версія API."""
    return {"message": "EventHub Pro API", "version": "1.0.0"}


@app.get("/health")
def health(services: Services = Depends(get_services)) -> Dict[str, str]:
    """Перевірка живості: ping до MongoDB"""
    services.repo.client.admin.command("ping")
    return {"status": "ok"}


# Users endpoints
@app.post("/users", status_code=201)
def create_user(user: UserCreate, services: Services = Depends(get_services)) -> Dict[str, str]:
    """Створити користувача; дубль email -> 400."""
    try:
        user_id = services.repo.create_user(user.model_dump(exclude_none=True))
        return {"id": str(user_id), "status": "created"}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/users/{user_id}")
def get_user(user_id: str, services: Services = Depends(get_services)) -> Dict[str, Any]:
    """Профіль користувача без password_hash."""
    user = services.repo.get_user_by_id(oid(user_id))
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    user.pop("password_hash", None)
    return to_json(user)


# Events endpoints
@app.post("/events", status_code=201)
def create_event(event: EventCreate, services: Services = Depends(get_services)) -> Dict[str, str]:
    """Створити подію; id організатора і дати приходять рядками."""
    data = event.model_dump(exclude_none=True)
    parse_object_ids(data, ["organizer.user_id"])
    for key in ("start_date", "end_date"):
        if isinstance(data["schedule"].get(key), str):
            data["schedule"][key] = datetime.fromisoformat(data["schedule"][key])
    try:
        event_id = services.repo.create_event(data)
        return {"id": str(event_id), "status": "created"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/events/search")
def search_events(
    q: str = "",
    category: Optional[str] = None,
    min_rating: Optional[float] = None,
    lng: Optional[float] = None,
    lat: Optional[float] = None,
    max_distance_km: Optional[float] = None,
    services: Services = Depends(get_services)
) -> Dict[str, Any]:
    """Пошук подій за текстом, категорією, рейтингом і відстанню."""
    coordinates = [lng, lat] if lng is not None and lat is not None else None
    events = services.repo.search_events(q, category=category, min_rating=min_rating,
                                         coordinates=coordinates, max_distance=max_distance_km)
    return {"results": to_json(events), "count": len(events)}


@app.get("/events/{event_id}/statistics")
def get_event_statistics(event_id: str, services: Services = Depends(get_services)) -> Dict[str, Any]:
    """Статистика події; немає події -> 404."""
    stats = services.analytics.get_event_statistics(oid(event_id))
    if not stats:
        raise HTTPException(status_code=404, detail="Event not found")
    return to_json(stats)


@app.post("/registrations", status_code=201)
def register_user(
    registration: RegistrationCreate,
    background_tasks: BackgroundTasks,
    services: Services = Depends(get_services)
) -> Dict[str, str]:
    """Зареєструвати користувача на подію і поставити лист у чергу."""
    try:
        reg_id = services.repo.register_user_for_event(
            oid(registration.user_id),
            oid(registration.event_id)
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    background_tasks.add_task(send_confirmation_email, services, registration)

    return {
        "id": str(reg_id),
        "status": "registered",
        "message": "Registration successful"
    }


# Reviews endpoints
@app.post("/reviews", status_code=201)
def create_review(review: ReviewCreate, services: Services = Depends(get_services)) -> Dict[str, str]:
    """Залишити відгук (лише підтверджений учасник, один раз)."""
    try:
        review_id = services.repo.create_review(oid(review.user_id), oid(review.event_id),
                                                review.rating, review.comment)
        return {"id": str(review_id), "status": "created"}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/analytics/categories")
def category_analytics(days: int = 30, services: Services = Depends(get_services)) -> Dict[str, Any]:
    """Аналітика за категоріями за останні days днів."""
    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=days)
    analytics = services.analytics.get_category_analytics(start_date, end_date)
    return {"period": f"Last {days} days", "data": to_json(analytics)}


@app.get("/analytics/recommendations/{user_id}")
def get_recommendations(user_id: str, limit: int = 10,
                        services: Services = Depends(get_services)) -> Dict[str, Any]:

    recommendations = services.analytics.get_recommendations(oid(user_id), limit)
    return {"user_id": user_id, "recommendations": to_json(recommendations)}



@app.post("/files/images", status_code=201)
async def upload_image(file: UploadFile = File(...),
                       services: Services = Depends(get_services)) -> Dict[str, Any]:
    """Завантажити зображення в GridFS разом із мініатюрами."""
    content = await file.read()
    try:
        ids = services.files.upload_image_bytes(
            content, file.filename or "image",
            {"content_type": file.content_type or "application/octet-stream"})
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Not an image: {e}")
    return to_json(ids)


@app.get("/files/{file_id}")
def download_file(file_id: str, services: Services = Depends(get_services)) -> StreamingResponse:
    """Віддати файл із GridFS потоком; немає файлу -> 404."""
    try:
        info = services.files.get_file_with_streaming(oid(file_id))
    except NoFile:
        raise HTTPException(status_code=404, detail="File not found")
    return StreamingResponse(info["stream"], media_type=info["content_type"],
                             headers={"Content-Length": str(info["size"])})


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
