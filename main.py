from pydantic import ValidationError
from src.kafka_broker import broker_manager
from src.kafka_broker.classes.event_object import EventObject
from src.kafka_broker.classes.event_router import EventRouter

event_router = EventRouter("test_router")

@event_router.bind_event("yeet")
def doe_iets(event_object: EventObject):
    raise ValidationError

event = EventObject(
    event="yeet",
)

print(event_router.binds)

event_router.execute_event(event)

print(broker_manager)