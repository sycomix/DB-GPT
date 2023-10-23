#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""The singleton metaclass for ensuring only one instance of a class."""
import abc
from typing import Any


class Singleton(abc.ABCMeta, type):
    """Singleton metaclass for ensuring only one instance of a class"""

    _instances = {}

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        """Call method for the singleton metaclass"""
        if self not in self._instances:
            self._instances[self] = super(Singleton, self).__call__(*args, **kwargs)
        return self._instances[self]


class AbstractSingleton(abc.ABC, metaclass=Singleton):
    """Abstract singleton class for ensuring only one instance of a class"""

    pass
