// Atlas Function: updateEventStatistics
// Викликається тригером update_event_stats (app/atlas.py: AtlasTriggers.build_trigger_config).
exports = async function (changeEvent) {
  const db = context.services.get("mongodb-atlas").db("eventhub");
  const events = db.collection("events");
  const eventId = changeEvent.fullDocument.event_id;

  const registrations = await db
    .collection("registrations")
    .find({ event_id: eventId, status: "confirmed" })
    .toArray();

  // Отримати всі відгуки
  const reviews = await db.collection("reviews").find({ event_id: eventId }).toArray();

  // Обчислити статистику
  const totalParticipants = registrations.length;
  const avgRating =
    reviews.length > 0 ? reviews.reduce((sum, r) => sum + r.rating, 0) / reviews.length : 0;

  // Оновити подію
  return events.updateOne(
    { _id: eventId },
    {
      $set: {
        "statistics.total_participants": totalParticipants,
        "statistics.average_rating": avgRating,
        "statistics.total_reviews": reviews.length,
        participants: registrations.map((r) => r.user_id)
      }
    }
  );
};
