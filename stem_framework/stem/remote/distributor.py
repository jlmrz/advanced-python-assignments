import asyncio
import logging
from asyncio import StreamReader, StreamWriter
from stem.meta import get_meta_attr
from stem.envelope import Envelope
from multiprocessing import Process


class Distributor:
    server = None

    def __init__(self, servers):
        self.servers = servers

    async def __call__(self, reader: StreamReader, writer: StreamWriter):
        logging.debug('Distributor is called')
        request = await Envelope.async_read(reader)
        if 'command' in request.meta:
            command = get_meta_attr(request.meta, 'command')
            logging.debug('Request command is ' + command)

            if command == 'run':
                response = Envelope(dict(status='not implemented'))

            elif command == 'structure':
                response = Envelope(dict(status='not implemented'))

            elif command == 'powerfullity':
                logging.debug('Nothing is done: command is not implemented')
                response = Envelope(dict(status='not implemented', powerfullity=None))

            elif command == 'stop':
                return None

            else:
                response = Envelope(dict(status='failed', error=f'Unknown command: {command}'))

            await response.async_write_to(writer)

        else:
            logging.debug('Command is not found in meta')
            await Envelope(dict(status='failed', error='Command is required')).async_write_to(writer)


async def start_distributor(host: str, port: int, servers: list[tuple[str, int]]):
    server = await asyncio.start_server(Distributor(servers), host, port)
    async with server:
        await server.serve_forever()


def _start_distributor(host: str, port: int, servers: list[tuple[str, int]]):
    asyncio.run(start_distributor(host, port, servers))


def start_distributor_in_subprocess(host: str, port: int, servers: list[tuple[str, int]]) -> Process:
    process = Process(target=_start_distributor, args=(host, port, servers), daemon=True)
    process.start()
    return process
