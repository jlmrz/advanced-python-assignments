import array
import mmap
import sys
import json
import struct
from asyncio import StreamReader, StreamWriter
from io import RawIOBase, BufferedReader, BytesIO, BufferedIOBase
from json import JSONEncoder
from typing import Optional, Union, Any
from .meta import Meta


Binary = Union[bytes, bytearray, memoryview, array.array, mmap.mmap]


class MetaEncoder(JSONEncoder):

    def default(self, obj: Meta) -> Any:
        if not isinstance(obj, dict):
            return vars(obj)
        else:
            return obj


class Envelope:
    _MAX_SIZE = 128*1024*1024  # 128 Mb

    def __init__(self, meta: Meta, data: Optional[Binary] = None):
        self.meta = meta
        if sys.getsizeof(data) > self._MAX_SIZE:
            datapath = '../env_data/data.txt'
            with open(datapath, 'wb') as file:
                file.write(data)
            with open(datapath, 'rb') as file:
                with mmap.mmap(file.fileno(), length=0, access=mmap.ACCESS_READ) as mmap_obj:
                    self.data = mmap_obj.read()
        else:
            self.data = data if data is not None else b''

    def __str__(self):
        return str(self.meta)

    @staticmethod
    def read(input: BufferedReader | BytesIO | BufferedIOBase) -> "Envelope":
        assert input.read(2) == b'~#', 'Wrong input'
        assert input.read(4) == b'DF02'
        input.read(2)
        meta_length = int.from_bytes(input.read(4), byteorder='big')
        data_length = int.from_bytes(input.read(4), byteorder='big')
        assert input.read(4) == b'~#\r\n'

        meta = input.read(meta_length)
        meta = json.loads(meta.decode('utf-8').replace("'", "\""))
        data = input.read(data_length)
        return Envelope(meta, data)

    @staticmethod
    def from_bytes(buffer: bytes) -> "Envelope":
        # Note: Use module struct for work with binary values. -- quote
        taglen, lenpoint = 20, 16
        (meta_length, data_length) = struct.unpack_from('>ii', buffer[:lenpoint], offset=8)
        meta = json.loads(buffer[taglen: taglen + meta_length].decode('utf-8').replace("'", "\""))
        data = buffer[taglen + meta_length: taglen + meta_length + data_length]
        return Envelope(meta=meta, data=data)

    def to_bytes(self) -> bytes:
        meta = json.dumps(self.meta, cls=MetaEncoder).encode(encoding='utf-8')
        tag = b'~#' + b'DF02' + b'..'
        tag += len(meta).to_bytes(4, byteorder='big')
        tag += len(self.data).to_bytes(4, byteorder='big')
        tag += b'~#\r\n'
        return tag + meta + self.data

    def write_to(self, output: RawIOBase):
        output.write(self.to_bytes())

    @staticmethod
    async def async_read(reader: StreamReader) -> "Envelope":
        assert await reader.read(2) == b'~#', 'Wrong input'
        assert await reader.read(4) == b'DF02'
        await reader.read(2)

        meta_length = int.from_bytes(await reader.read(4), byteorder='big')
        data_length = int.from_bytes(await reader.read(4), byteorder='big')
        assert await reader.read(4) == b'~#\r\n'

        meta = await reader.read(meta_length)
        meta = json.loads(meta.decode('utf-8').replace("'", "\""))
        data = await reader.read(data_length)
        return Envelope(meta, data)

    async def async_write_to(self, writer: StreamWriter):
        writer.write(self.to_bytes())
        await writer.drain()

