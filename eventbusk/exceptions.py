"""
Custom exceptions
"""
from __future__ import annotations


__all__ = [
    "AgentError",
    "AlreadyRegistered",
    "EventBusError",
    "UnRegisteredEvent",
]

# TODO: Figure out better names
class EventBusError(Exception):
    pass


class UnRegisteredEvent(EventBusError):
    pass


class AlreadyRegistered(EventBusError):
    pass


class AgentError(EventBusError):
    pass
