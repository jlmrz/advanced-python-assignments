"""
Modularity concept means that the core of the system contains only basic functionality for working, and
all specific functions, such as the graphical environment or tools, are placed in separate plug-ins.
"""
from abc import abstractmethod, ABC, ABCMeta
from types import ModuleType
from typing import Optional, Any, TypeVar, Union
from inspect import isclass, getmodule
from .core import Named
from .meta import Meta
from .task import Task

T = TypeVar("T")


class TaskPath:
    def __init__(self, path: Union[str, list[str]]):
        if isinstance(path, str):
            self._path = path.split(".")
        else:
            self._path = path

    @property
    def is_leaf(self):
        return len(self._path) == 1

    @property
    def sub_path(self):
        return TaskPath(self._path[1:])

    @property
    def head(self):
        return self._path[0]

    @property
    def name(self):
        return self._path[-1]

    def __str__(self):
        return ".".join(self._path)


class ProxyTask(Task[T]):

    def __init__(self, proxy_name, task: Task):
        self._name = proxy_name
        self._task = task

    @property
    def dependencies(self):
        return self._task.dependencies

    @property
    def specification(self):
        return self._task.specification

    def check_by_meta(self, meta: Meta):
        self._task.check_by_meta(meta)

    def transform(self, meta: Meta, /, **kwargs: Any) -> T:
        return self._task.transform(meta, **kwargs)


class IWorkspace(ABC, Named):
    _tasks: dict[str, Task] = NotImplemented
    _workspaces: set['IWorkspace'] = NotImplemented

    @property
    @abstractmethod
    def tasks(self) -> dict[str, Task]:
        pass

    @property
    @abstractmethod
    def workspaces(self) -> set["IWorkspace"]:
        pass

    def find_task(self, task_path: Union[str, TaskPath]) -> Optional[Task]:
        if isinstance(task_path, str):
            task_path = TaskPath(task_path)
        if task_path.is_leaf:
            task = self.tasks.get(task_path.name, None)
            if task is None:
                for w in self.workspaces:
                    task = w.find_task(task_path)
            return task
        else:
            workspace = self.get_workspace(task_path.head)
            return workspace.find_task(task_path.sub_path)

    def has_task(self, task_path: Union[str, TaskPath]) -> bool:
        return self.find_task(task_path) is not None

    def get_workspace(self, name) -> Optional["IWorkspace"]:
        for workspace in self.workspaces:
            if workspace.name == name:
                return workspace
        return None

    def structure(self) -> dict:
        return {
            "name": self.name,
            "tasks": list(self.tasks.keys()),
            "workspaces": [w.structure() for w in self.workspaces]
        }

    @staticmethod
    def find_default_workspace(task: Task) -> "IWorkspace":
        if hasattr(task, '_stem_workspace'):
            return getattr(task, '_stem_workspace')
        else:
            if hasattr(task, '_func'):
                module = getmodule(getattr(task, '_func'))
                return IWorkspace.module_workspace(module)

    @staticmethod
    def module_workspace(module: ModuleType) -> "IWorkspace":
        filename = module.__name__.split('.').pop()
        tasks = {}
        workspaces = set()
        for name in dir(module):
            t = getattr(module, name)
            if callable(t) and issubclass(type(t), Task):
                tasks[name] = t
            elif isclass(t) and t == IWorkspace:
                workspaces.add(t)

        return LocalWorkspace(filename, tasks, workspaces)


class ILocalWorkspace(IWorkspace):
    @property
    def tasks(self) -> dict[str, Task]:
        return self._tasks

    @property
    def workspaces(self) -> set["IWorkspace"]:
        return self._workspaces


class LocalWorkspace(ILocalWorkspace):
    def __init__(self, name, tasks=(), workspaces=()):
        self._name = name
        self._tasks = tasks
        self._workspaces = workspaces


class Workspace(ABCMeta, ILocalWorkspace):
    def __new__(mcls, name: str, bases: tuple[type, ...], namespace: dict[str, Any], **kwargs: Any):
        # Class-objects of user classes implement the interface ILocalWorkspace
        if ILocalWorkspace not in bases:
            bases += (ILocalWorkspace,)
        cls = super().__new__(mcls, name, bases, namespace, **kwargs)

        _workspaces = cls.__dict__.get('workspaces', ())
        cls.__call__ = lambda userclass: userclass

        _tasks = {n: t for n, t in cls.__dict__.items() if isinstance(t, Task)}

        cls_inst = cls()
        cls_inst._name = name
        cls_inst._tasks = _tasks
        cls_inst._workspaces = set(_workspaces)

        for n, t in cls.__dict__.items():
            if isinstance(t, Task):
                setattr(cls_inst, n, ProxyTask(n, t))   # both methods and attributes are proxied
                getattr(cls_inst, n)._stem_workspace = cls_inst

        return cls_inst

