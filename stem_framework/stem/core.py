"""
Core module.
"""
from typing import Optional
from dataclasses import dataclass
import re


def pascal_case_to_snake_case(name: str) -> str:
    half_snake = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', half_snake).lower()


class Named:
    _name: Optional[str] = None

    @property
    def name(self):
        if self._name is not None:
            return self._name
        else:
            name = self.__class__.__name__
            print(name)
            return pascal_case_to_snake_case(name)


@dataclass
class Dataclass:
    ...  # TODO()