import logging

from socketserver import StreamRequestHandler, TCPServer
from typing import Optional

from stem.envelope import Envelope
from stem.task_master import TaskMaster
from stem.task_runner import SimpleRunner
from stem.task_tree import TaskTree
from stem.workspace import IWorkspace
from multiprocessing import Process
from stem.meta import get_meta_attr


# only for the distributor testing
class Commands:
    powerfullity = Envelope(dict(command='powerfullity'))


class UnitHandler(StreamRequestHandler):
    workspace: IWorkspace
    task_tree: TaskTree = None  # TaskTree()
    task_master = TaskMaster(SimpleRunner(), task_tree)
    powerfullity: int

    def handle(self) -> None:
        # I'm not sure if this works fine, but user should run tests separately,
        #   otherwise it runs into "OSError: [Errno 98] Address already in use"
        logging.debug('Handle started')
        # self.rfile is a file-like object created by the handler;
        # supports the io.BufferedIOBase readable interface.
        request = Envelope.read(self.rfile)
        try:
            command = get_meta_attr(request.meta, 'command')
        except AttributeError:
            self.wfile.write(Envelope(dict(status='failed', error=AttributeError)).to_bytes())
            return None
        logging.debug('Request command is ' + command)
        if command == 'run':
            if 'task_path' in request.meta:
                task_path = get_meta_attr(request.meta, 'task_path')
                task = self.workspace.find_task(task_path)
                if task is None:
                    response = Envelope(dict(status='failed', error='Task not found')).to_bytes()
                else:
                    task_result = self.task_master.execute(request.meta, task, self.workspace)
                    response = Envelope(vars(task_result)).to_bytes()
            else:
                response = Envelope(dict(status='failed', error='Task not found')).to_bytes()
        elif command == 'structure':
            response = Envelope(self.workspace.structure()).to_bytes()
        elif command == 'powerfullity':
            response = Envelope(dict(status='success', powerfullity=self.powerfullity)).to_bytes()
        elif command == 'stop':
            logging.debug('Stopping server')
            self.server.shutdown()
            self.server.server_close()
            return None
        else:
            response = Envelope(dict(status='failed', error='Unknown command')).to_bytes()
        self.wfile.write(response)


def start_unit(workspace: IWorkspace, host: str, port: int, powerfullity: Optional[int] = None):
    # create TCP server
    UnitHandler.workspace = workspace
    UnitHandler.powerfullity = powerfullity

    with TCPServer((host, port), UnitHandler) as server:
        server.allow_reuse_address = True
        server.serve_forever()


def start_unit_in_subprocess(workspace: IWorkspace, host: str, port: int, powerfullity: Optional[int] = None) -> Process:
    process = Process(target=start_unit, args=(workspace, host, port, powerfullity), daemon=True)
    process.start()
    return process
