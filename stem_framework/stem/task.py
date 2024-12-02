"""
This module contains tasks which are global data processing blocks.
"""
from typing import TypeVar, Union, Tuple, Callable, Optional, Generic, Any, Iterator
from abc import ABC, abstractmethod
from inspect import signature
from stem_framework.stem.core import Named
from stem_framework.stem.meta import Specification, Meta

T = TypeVar("T")


class Task(ABC, Generic[T], Named):
    dependencies: Tuple[Union[str, "Task"], ...]
    specification: Optional[Specification] = None
    settings: Optional[Meta] = None

    def check_by_meta(self, meta: Meta):
        pass

    @abstractmethod
    def transform(self, meta: Meta, /, **kwargs: Any) -> T:
        pass


class FunctionTask(Task[T]):
    def __init__(self, name: str, func: Callable, dependencies: Tuple[Union[str, "Task"], ...],
                 specification: Optional[Specification] = None,
                 settings: Optional[Meta] = None):
        self._name = name
        self._func = func
        self.dependencies = dependencies
        self.specification = specification
        self.settings = settings

    def __call__(self, *args, **kwargs):
        return self._func(*args, **kwargs)

    def transform(self, meta: Meta, /, **kwargs: Any) -> T:
        return self._func(meta, **kwargs)


class DataTask(Task[T]):
    dependencies = ()

    @abstractmethod
    def data(self, meta: Meta) -> T:
        pass

    def transform(self, meta: Meta, /, **kwargs: Any) -> T:
        return self.data(meta)


class FunctionDataTask(DataTask[T]):
    def __init__(self, name: str, func: Callable,
                 specification: Optional[Specification] = None,
                 settings: Optional[Meta] = None):
        self._name = name
        self._func = func
        self.specification = specification
        self.settings = settings

    def __call__(self, *args, **kwargs):
        return self._func(*args, **kwargs)

    def data(self, meta: Meta) -> T:
        return self._func(meta)


def data(func: Callable[[Meta], T], specification: Optional[Specification] = None, **settings) -> FunctionDataTask[T]:
    return FunctionDataTask(
        name=func.__name__,
        func=func,
        specification=specification,
        settings=settings
    )


def task(func: Callable[[Meta, ...], T], specification: Optional[Specification] = None, **settings) -> FunctionTask[T]:
    dependencies = tuple(name for name in signature(func).parameters.keys() if name != 'meta')
    return FunctionTask(
        name=func.__name__,
        func=func,
        dependencies=dependencies,
        specification=specification,
        settings=settings
    )


class MapTask(Task[Iterator[T]]):
    def __init__(self, func: Callable, dependence: Union[str, "Task"]):
        self._func = func
        self.dependencies = dependence
        self._name = 'map_' + dependence.name

    def transform(self, meta: Meta, **kwargs):
        for dependence in self.dependencies.transform(meta, **kwargs):
            yield self._func(dependence)


class FilterTask(Task[Iterator[T]]):
    def __init__(self, key: Callable, dependence: Union[str, "Task"]):
        self._name = 'filter_' + dependence.name
        self.dependencies = dependence
        self.key = key

    def transform(self, meta: Meta, /, **kwargs: Any) -> T:
        for dependence in self.dependencies.transform(meta, **kwargs):
            if self.key(dependence):
                yield dependence


class ReduceTask(Task[Iterator[T]]):
    def __init__(self, func: Callable, dependence: Union[str, "Task"]):
        self._name = 'reduce_' + dependence.name
        self.dependencies = dependence
        self.func = func

    def transform(self, meta: Meta, /, **kwargs: Any) -> T:
        iterator = self.dependencies.transform(meta, **kwargs)
        value = next(iterator)
        for dependence in iterator:
            value = self.func(value, dependence)
        return value
