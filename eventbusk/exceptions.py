"""
Custom exceptions
"""

__all__ = [
    "EventBusError",
    "AlreadyRegistered",
    "AgentError"
]

class EventBusError(Exception):
    pass


class AlreadyRegistered(EventBusError):
    pass


class AgentError(EventBusError):
    pass
