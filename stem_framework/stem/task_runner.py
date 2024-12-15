import os
import asyncio

from typing import Generic, TypeVar
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from multiprocessing.pool import ThreadPool

from .meta import Meta, get_meta_attr
from .task_tree import TaskNode

T = TypeVar("T")


class TaskRunner(ABC, Generic[T]):

    @abstractmethod
    def run(self, meta: Meta, task_node: TaskNode[T]) -> T:
        pass


class SimpleRunner(TaskRunner[T]):
    def run(self, meta: Meta, task_node: TaskNode[T]) -> T:
        kwargs_tree = {
            node.task.name: self.run(get_meta_attr(meta, node.task.name, {}), node)
            for node in task_node.dependencies
        }
        return task_node.task.transform(meta, **kwargs_tree)


class ThreadingRunner(TaskRunner[T]):
    MAX_WORKERS = 5

    def run(self, meta: Meta, task_node: TaskNode[T]) -> T:
        with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
            future_runs = list(executor.map(
                self.run,
                [get_meta_attr(meta, node.task.name, {}) for node in task_node.dependencies],
                task_node.dependencies
            ))
        kwargs_tree = {
            node.task.name: _run
            for node, _run in zip(task_node.dependencies, future_runs)
        }
        return task_node.task.transform(meta, **kwargs_tree)


class AsyncRunner(TaskRunner[T]):
    def run(self, meta: Meta, task_node: TaskNode[T]) -> T:
        return asyncio.run(self._run(meta, task_node))

    async def _run(self, meta: Meta, task_node: TaskNode[T]) -> T:
        # Example reference:
        # https: // docs.python.org / 3 / library / asyncio - task.html  # asyncio.TaskGroup.create_task
        async with asyncio.TaskGroup() as tg:
            kwargs_tasks = {
                node.task.name: tg.create_task(self._run(get_meta_attr(meta, node.task.name, {}), node))
                for node in task_node.dependencies
                }
        kwargs_tree = {name: async_task.result() for name, async_task in kwargs_tasks.items()}
        return task_node.task.transform(meta, **kwargs_tree)


class ProcessingRunner(TaskRunner[T]):
    MAX_WORKERS = os.cpu_count()

    def run(self, meta: Meta, task_node: TaskNode[T]) -> T:
        with ThreadPool(self.MAX_WORKERS) as pool:
            future_runs = list(pool.starmap(
                self.run,
                zip(
                    [get_meta_attr(meta, node.task.name, {}) for node in task_node.dependencies],
                    task_node.dependencies
                )
            ))
        kwargs_tree = {
            node.task.name: _run
            for node, _run in zip(task_node.dependencies, future_runs)
        }
        return task_node.task.transform(meta, **kwargs_tree)

