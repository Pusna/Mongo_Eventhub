"""Розділ 2.3. Налаштування шардінгу: ключі для events/users і зони EU/US."""
from bson.max_key import MaxKey
from bson.min_key import MinKey
from pymongo import MongoClient

from app.config import settings


def setup_sharding() -> None:
    """Увімкнути шардінг eventhub, шардувати колекції та налаштувати зони."""
    # Авторизація: логін і пароль передаються в URI з config
    client = MongoClient(settings.mongos_uri)

    client.admin.command("enableSharding", "eventhub")

    client.admin.command(
        "shardCollection",
        "eventhub.events",
        key={"_id": "hashed"},
        unique=False
    )

    client.admin.command(
        "shardCollection",
        "eventhub.users",
        key={"address.country": 1, "address.city": 1}
    )

    client.admin.command("addShardToZone", "shard1ReplSet", zone="EU")
    client.admin.command("addShardToZone", "shard2ReplSet", zone="US")

    client.admin.command(
        "updateZoneKeyRange",
        "eventhub.users",
        min={"address.country": "Ukraine", "address.city": MinKey()},
        max={"address.country": "Ukraine", "address.city": MaxKey()},
        zone="EU"
    )

    print("Sharding configured successfully!")


if __name__ == "__main__":
    setup_sharding()
