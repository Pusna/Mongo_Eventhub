"""Розширені індекси, explain і оптимізація запитів."""
from pymongo import ASCENDING, DESCENDING, MongoClient
from pymongo.database import Database
from pymongo.errors import OperationFailure
from typing import Any, Dict, List, Tuple
import ast
import logging

from app.config import settings

logger = logging.getLogger(__name__)


def _plan_has_stage(plan: Any, stage_name: str) -> bool:
    """Рекурсивно шукає стадію (наприклад COLLSCAN) у всьому плані виконання"""
    if isinstance(plan, dict):
        if plan.get("stage") == stage_name:
            return True
        return any(_plan_has_stage(v, stage_name) for v in plan.values())
    if isinstance(plan, list):
        return any(_plan_has_stage(v, stage_name) for v in plan)
    return False


class IndexOptimizer:
    """Аналіз повільних запитів і створення індексів (розділ 4)."""

    def __init__(self, db: Database) -> None:
        self.db = db

    # ---------- Аналіз та створення оптимальних індексів ----------

    def analyze_query_patterns(self) -> List[Dict]:
        """Аналіз повільних запитів з system.profile"""
        self.db.command("profile", 1, slowms=100)
        slow_queries = self.db.system.profile.find({
            "op": "query",
            "millis": {"$gt": 100}
        }).sort([("ts", DESCENDING)]).limit(100)

        patterns = {}
        for query in slow_queries:
            query_filter = query.get("command", {}).get("filter", query.get("query", {}))

            key = str({field: 1 for field in query_filter})
            patterns[key] = patterns.get(key, 0) + 1

        recommendations = []
        for pattern, count in sorted(patterns.items(), key=lambda x: x[1], reverse=True)[:10]:
            if count > 10:
                recommendations.append({
                    "pattern": pattern,
                    "frequency": count,
                    "recommended_index": self._suggest_index(pattern)
                })
        return recommendations

    def _suggest_index(self, query_pattern: str) -> Dict:
        """Рекомендувати індекс на основі патерну запиту"""
        try:
            query_dict = ast.literal_eval(query_pattern)
            fields = []
            for key in query_dict.keys():
                if key.startswith("$"):
                    continue
                fields.append((key, ASCENDING))
            return {"fields": fields}
        except Exception:
            return {"fields": []}

    def create_compound_indexes(self) -> None:
        """Створення складених індексів для типових запитів"""

        # Для пошуку подій за категорією та датою.
        self.db.events.create_index([
            ("category", ASCENDING),
            ("schedule.start_date", ASCENDING),
            ("statistics.average_rating", DESCENDING)
        ], name="category_rating_date_index")

        # Для звітів по користувачах
        self.db.registrations.create_index([
            ("user_id", ASCENDING),
            ("status", ASCENDING),
            ("registered_at", DESCENDING)
        ])

        # Для пошуку відгуків з фільтрацією
        self.db.reviews.create_index([
            ("event_id", ASCENDING),
            ("rating", DESCENDING),
            ("created_at", DESCENDING)
        ])
        logger.info("Складені індекси створено")

    def test_index_coverage(self) -> Dict:
        """Перевірка, чи покривають індекси запити"""
        result = self.db.events.find(
            {"category": "conference"},
            {"title": 1, "category": 1, "_id": 0}
        ).explain()

        docs_examined = result.get("executionStats", {}).get("totalDocsExamined")

        if docs_examined == 0:
            print("Covered query detected!")
        else:
            print(f"Documents examined: {docs_examined}")
            print("   (title немає в жодному звичайному індексі разом з category, "
                  "тому запит не може бути покритим)")

        return result

    # ---------- Partial та Wildcard індекси ----------

    def create_special_indexes(self) -> None:
        """Створення спеціальних індексів"""
        db = self.db

        # Partial index - тільки для активних подій
        db.events.create_index(
            [("schedule.start_date", ASCENDING)],
            partialFilterExpression={
                "status": {"$in": ["published", "active"]}
            },
            name="active_events_date_index"
        )

        # Wildcard index - для динамічних полів в metadata
        db.events.create_index(
            [("metadata.$**", ASCENDING)],
            name="metadata_wildcard_index"
        )

        # Гео-індекс з додатковими полями
        db.events.create_index(
            [
                ("location.coordinates", "2dsphere"),
                ("category", ASCENDING),
                ("schedule.start_date", ASCENDING)
            ],
            name="geo_category_date_index"
        )

        # TTL індекс для тимчасових даних (90 днів; див. DEVIATIONS R1)
        db.registrations.create_index(
            [("registered_at", ASCENDING)],
            expireAfterSeconds=7776000
        )

        # Унікальний частковий індекс.
        try:
            db.users.create_index(
                [("email", ASCENDING)],
                unique=True,
                partialFilterExpression={"status": {"$in": ["active", "suspended"]}},
                name="active_users_email_unique"
            )
        except OperationFailure as e:

            logger.warning(f"active_users_email_unique не створено: {e}")

        logger.info("Спеціальні індекси створено")

    # ---------- Explain план виконання ----------

    def analyze_query_performance(self) -> Tuple[Dict, Dict]:
        """Детальний аналіз продуктивності запиту"""
        query = {"category": "conference", "statistics.average_rating": {"$gte": 4.0}}
        sort_doc = [("schedule.start_date", ASCENDING)]

        explain_result = self.db.events.find(query).sort(sort_doc).explain()

        print("\n=== Аналіз продуктивності запиту ===")
        print(f"Запит: {query}")
        print(f"Сортування: {sort_doc}")
        self._print_stats(explain_result)

        plan = explain_result.get("queryPlanner", {}).get("winningPlan", {})
        if _plan_has_stage(plan, "COLLSCAN"):
            print("УВАГА: Виявлено COLLSCAN (повне сканування таблиці)! Потрібно додати індекс.")
        else:
            print(f"База використовує індекс. Верхня стадія: {plan.get('stage')}")

        hint_result = self.db.events.find(query).sort(sort_doc).hint(
            "category_rating_date_index"
        ).explain()

        print("\n--- З hint('category_rating_date_index') ---")
        self._print_stats(hint_result)

        return explain_result, hint_result

    @staticmethod
    def _print_stats(explain_result: Dict) -> None:
        """Надрукувати головні цифри executionStats."""
        stats = explain_result.get("executionStats", {})
        print(f"Переглянуто документів у базі: {stats.get('totalDocsExamined')}")
        print(f"Переглянуто ключів індексу: {stats.get('totalKeysExamined')}")
        print(f"Час виконання: {stats.get('executionTimeMillis')} мс")
