import dataclasses
import enum
from concurrent.futures.thread import ThreadPoolExecutor
from typing import Any

import qcmd.processors.executor as ep


class CommandId(enum.Enum):
    MULT = enum.auto()
    ADD = enum.auto()


_SampleCommand = ep.Command[CommandId, Any, int]
SampleCommandHandle = ep.CommandHandle[CommandId, int]
SampleProcessorFactory = ep.ProcessorFactory[CommandId, Any]

R = int


class Cmd:
    @dataclasses.dataclass
    class Mult(_SampleCommand):
        cmdid = CommandId.MULT

        x: int
        y: int

        def exec(self, hcmd: SampleCommandHandle, cxt: None = None) -> R:
            return self.x * self.y

    @dataclasses.dataclass
    class Add(_SampleCommand):
        cmdid = CommandId.ADD

        x: int
        y: int

        def exec(self, hcmd: SampleCommandHandle, cxt: None = None) -> R:
            return self.x + self.y


def test_sample_simple():
    with ThreadPoolExecutor(1, thread_name_prefix="Sample") as executor:
        with SampleProcessorFactory(executor=executor, name="Sample", cxt=None) as sp:
            # the context manager starts the processor automatically
            assert not sp.paused()

            addran = 0

            def _assert_add(x: int, y: int, result: int):
                nonlocal addran
                assert result == x + y
                addran += 1

            multran = 0

            def _assert_mult(x: int, y: int, result: int):
                nonlocal multran
                assert result == x * y
                multran += 1

            x = 1
            y = 2

            sp.send(Cmd.Add(x, y)).then(lambda result, tags: _assert_add(x, y, result))

            sp.send(Cmd.Mult(x, y)).then(lambda result, _: _assert_mult(x, y, result))

            sp.join()
            sp.pause()
            assert sp.paused()

            assert addran == 1
            assert multran == 1

            # processor can be un-paused with start()
            sp.start()

            x = 5
            y = 6

            sp.send(Cmd.Add(x, y)).then(lambda result, tags: _assert_add(x, y, result))

            sp.send(Cmd.Mult(x, y)).then(lambda result, _: _assert_mult(x, y, result))

            sp.join()

            assert addran == 2
            assert multran == 2
