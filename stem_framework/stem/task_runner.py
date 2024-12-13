import os
from typing import Generic, TypeVar
from abc import ABC, abstractmethod

from .meta import Meta
from .task_tree import TaskNode

T = TypeVar("T")


class TaskRunner(ABC, Generic[T]):

    @abstractmethod
    def run(self, meta: Meta, task_node: TaskNode[T]) -> T:
        pass


class SimpleRunner(TaskRunner[T]):
    def run(self, meta: Meta, task_node: TaskNode[T]) -> T:
        kwargs_tree = {
            node.task.name: self.run(getattr(meta, node.task.name, {}), node) for node in task_node.dependencies
        }
        return task_node.task.transform(meta, **kwargs_tree)


class ThreadingRunner(TaskRunner[T]):
    MAX_WORKERS = 5

    def run(self, meta: Meta, task_node: TaskNode[T]) -> T:
        pass  # TODO(Assignment 9)


class AsyncRunner(TaskRunner[T]):
    def run(self, meta: Meta, task_node: TaskNode[T]) -> T:
        pass  # TODO(Assignment 9)


class ProcessingRunner(TaskRunner[T]):
    MAX_WORKERS = os.cpu_count()

    def run(self, meta: Meta, task_node: TaskNode[T]) -> T:
        pass  # TODO(Assignment 9)
