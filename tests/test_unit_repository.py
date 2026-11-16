"""Unit Testing"""
import unittest
from datetime import datetime, timedelta, timezone

from app.config import settings
from app.operations import EventHubOperations

TEST_EMAILS = ["test@example.com", "user@test.com", "user2@test.com", "test_unit@example.com"]
TEST_TITLES = ["Test Event", "Unit Test Event"]


def event_data(title: str, total: int, organizer_id) -> dict:
    return {
        "title": title,
        "category": "workshop",
        "schedule": {"start_date": datetime.now(timezone.utc) + timedelta(days=1)},
        "capacity": {"total": total},
        "organizer": {"user_id": organizer_id},
    }


class TestEventHubRepository(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Підготовка до тестів (одне підключення на клас)"""
        cls.repo = EventHubOperations(db_name=settings.mongo_test_db)
        cls.db = cls.repo.db

    @classmethod
    def tearDownClass(cls):
        cls.repo.client.close()

    def setUp(self):
        events = [e["_id"] for e in self.db.events.find({"title": {"$in": TEST_TITLES}}, {"_id": 1})]
        users = [u["_id"] for u in self.db.users.find({"email": {"$in": TEST_EMAILS}}, {"_id": 1})]
        self.db.registrations.delete_many({"$or": [{"event_id": {"$in": events}},
                                                   {"user_id": {"$in": users}}]})
        self.db.events.delete_many({"_id": {"$in": events}})
        self.db.users.delete_many({"_id": {"$in": users}})

    def test_create_user(self):
        """Тест створення користувача"""
        user_id = self.repo.create_user({"email": "test@example.com",
                                         "profile": {"name": "Test User"}})
        self.assertIsNotNone(user_id)

        user = self.db.users.find_one({"_id": user_id})
        self.assertEqual(user["email"], "test@example.com")
        self.assertEqual(user["status"], "active")
        self.assertEqual(user["version"], 1)

    def test_duplicate_email(self):
        """Тест на дублювання email"""
        self.repo.create_user({"email": "test@example.com"})

        with self.assertRaises(ValueError):
            self.repo.create_user({"email": "test@example.com"})

    def test_register_for_event(self):
        """Тест реєстрації на подію"""
        user_id = self.repo.create_user({"email": "user@test.com"})
        event_id = self.repo.create_event(event_data("Test Event", 10, user_id))

        registration_id = self.repo.register_user_for_event(user_id, event_id)
        self.assertIsNotNone(registration_id)

        event = self.db.events.find_one({"_id": event_id})
        self.assertEqual(event["capacity"]["remaining"], 9)
        self.assertIn(user_id, event["participants"])

    def test_duplicate_registration(self):
        """Тест на дублювання реєстрації"""
        user_id = self.repo.create_user({"email": "user@test.com"})
        event_id = self.repo.create_event(event_data("Test Event", 10, user_id))

        self.repo.register_user_for_event(user_id, event_id)

        with self.assertRaises(ValueError):
            self.repo.register_user_for_event(user_id, event_id)

    def test_event_capacity_limit(self):
        """Тест ліміту capacity"""
        user_id = self.repo.create_user({"email": "user@test.com"})
        event_id = self.repo.create_event(event_data("Test Event", 1, user_id))

        self.repo.register_user_for_event(user_id, event_id)

        user2_id = self.repo.create_user({"email": "user2@test.com"})
        with self.assertRaises(ValueError):
            self.repo.register_user_for_event(user2_id, event_id)

        event = self.db.events.find_one({"_id": event_id})
        self.assertEqual(event["capacity"]["remaining"], 0)


    def test_create_user_and_duplicate(self):
        """Створення юзера та перевірка на дублікат email"""
        user_data = {"email": "test_unit@example.com", "profile": {"name": "Test User"}}
        self.assertIsNotNone(self.repo.create_user(dict(user_data)))
        with self.assertRaises(ValueError):
            self.repo.create_user(dict(user_data))

    def test_create_event_capacity(self):
        """Створення події: remaining = total, статус за замовчуванням"""
        event_id = self.repo.create_event(event_data("Unit Test Event", 10, None))
        event = self.db.events.find_one({"_id": event_id})
        self.assertEqual(event["capacity"]["remaining"], 10)
        self.assertEqual(event["status"], "published")
