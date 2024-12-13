from typing import TypeVar, Optional, Generic

from .task import Task
from .workspace import IWorkspace

T = TypeVar("T")


class TaskNode(Generic[T]):
    def __init__(self, task: Task, workspace: IWorkspace) -> None:
        self.task = task
        self._workspace = IWorkspace.find_default_workspace(task) if workspace is None else workspace
        self._dependencies = TaskNode.set_resolved(task, self._workspace)
        self._unresolved_dependencies = TaskNode.set_unresolved(task, self._workspace)

    @property
    def dependencies(self) -> list["TaskNode"]:
        return self._dependencies

    @property
    def workspace(self) -> IWorkspace:
        return self._workspace

    @property
    def is_leaf(self) -> bool:
        return len(self._dependencies) == 0

    @property
    def unresolved_dependencies(self) -> list["str"]:
        return self._unresolved_dependencies

    @property
    def has_dependence_errors(self) -> bool:
        return len(self.unresolved_dependencies) != 0

    @staticmethod
    def set_resolved(task: Task[T], workspace: IWorkspace) -> list["TaskNode"]:
        _resolved = []
        for taskpath in task.dependencies:
            if workspace.has_task(taskpath):
                _resolved.append(TaskNode(workspace.find_task(taskpath), workspace))
        return _resolved

    @staticmethod
    def set_unresolved(task: Task[T], workspace: IWorkspace) -> list["str"]:
        _unresolved_dependencies = []
        for taskpath in task.dependencies:
            if not workspace.has_task(taskpath):
                _unresolved_dependencies.append(task.name)
            elif workspace.has_task(taskpath) and len(task.dependencies) != 0:
                task = workspace.find_task(taskpath)
                _unresolved_dependencies.extend(TaskNode.set_unresolved(task, workspace))
        return _unresolved_dependencies


class TaskTree:
    def __init__(self, task: Task[T], workspace: Optional[IWorkspace] = None) -> None:
        self.root = TaskTree.build_node(task, workspace)

    @staticmethod
    def build_node(task: Task[T], workspace: Optional[IWorkspace] = None) -> TaskNode[T]:
        return TaskNode(task, workspace)

    def find_task(self, task: Task[T],  workspace: Optional[IWorkspace] = None) -> TaskNode[T]:
        if task == self.root.task and workspace == self.root.workspace:
            return self.root
        else:
            for dependency in self.root.dependencies:
                subtree = TaskTree(dependency.task, dependency.workspace)
                node = subtree.find_task(task, workspace)
                if node is not None:
                    return node

    def resolve_node(self, task: Task[T], workspace: Optional[IWorkspace] = None) -> TaskNode[T]:
        workspace = IWorkspace.find_default_workspace(task) if workspace is None else workspace
        node = self.find_task(task, workspace)
        return node
