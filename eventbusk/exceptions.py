"""
Custom exceptions
"""
from __future__ import annotations


__all__ = ["EventBusError", "AlreadyRegistered", "UnknownEvent", "AgentError"]


class EventBusError(Exception):
    pass


class UnknownEvent(EventBusError):
    pass


class AlreadyRegistered(EventBusError):
    pass


class AgentError(EventBusError):
    pass
