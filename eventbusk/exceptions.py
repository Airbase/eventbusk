"""
Custom exceptions
"""
from __future__ import annotations

__all__ = [
    "AlreadyRegistered",
    "ConsumerError",
    "EventBusError",
    "UnknownEvent",
]


class EventBusError(Exception):
    """
    Base of exceptions raised by the bus.
    """


class UnknownEvent(EventBusError):
    """
    Raised when an receiver is created for an event the bus does not recognize.
    """


class AlreadyRegistered(EventBusError):
    """
    Raised when an event is registered more than once to the bus.
    """


class ProducerError(EventBusError):
    """
    Raised during production of an event.
    """


class ConsumerError(EventBusError):
    """
    Raised during consumption of an event
    """
