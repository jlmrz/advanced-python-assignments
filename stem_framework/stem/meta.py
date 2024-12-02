"""
Metadata is data that provides information about other data.
Metadata processor is a principle of data processing when no user instructions or manually managed
intermediate states are possible. It allows using only data or metadata.
"""
from dataclasses import dataclass, astuple, asdict
import dataclasses
from typing import Optional, Any, Union, Tuple
from stem_framework.stem.core import Dataclass


Meta = Union[Dataclass, dict]

SpecificationField = Tuple[str, Union[float, Tuple[Any], Meta]]

Specification = Union[Dataclass, Tuple[SpecificationField]]


class SpecificationError(Exception):
    pass


@dataclass
class MetaFieldError:
    required_key: str
    required_types: Optional[tuple[type]] = None
    presented_type: Optional[type] = None
    presented_value: Any = None


class MetaVerification:

    def __init__(self, *errors: Union[MetaFieldError, "MetaVerification"]):
        self.error = errors
        self.checked_success = len(errors) == 0

    @staticmethod
    def verify(meta: Meta, specification: Optional[Specification] = None) -> "MetaVerification":
        errors = []
        if dataclasses.is_dataclass(specification):
            specification = tuple((key, type(value)) for key, value in specification.__dict__.items() if key[0] != '_')

        for pair in specification:
            (required_key, required_types) = pair
            presented_type = type(get_meta_attr(meta, required_key))

            if isinstance(required_types, type):
                required_types = (required_types, )

            if isinstance(required_types, tuple):
                if presented_type not in required_types:
                    error = MetaFieldError(required_key, required_types, presented_type)
                    errors.append(error)
            else:
                verification = MetaVerification.verify(meta, specification)
                if verification.checked_success:
                    errors.append(verification)

        return MetaVerification(*errors)


def get_meta_attr(meta: Meta, key: str, default: Optional[Any] = None) -> Optional[Any]:
    if isinstance(meta, dict):
        return meta.get(key, default)
    else:
        return getattr(meta, key, default)


def update_meta(meta: Meta, **kwargs):
    if isinstance(meta, dict):
        meta.update(kwargs)
    for k, v in kwargs.items():
        setattr(meta, k, v)
