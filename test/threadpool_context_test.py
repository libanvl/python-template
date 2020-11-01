import dataclasses
import enum
from concurrent.futures.thread import ThreadPoolExecutor

import qcmd.processors.executor as ep


class CommandId(enum.Enum):
    MULT = enum.auto()
    ADD = enum.auto()


X = int

_SampleCommand = ep.Command[CommandId, X, int]
SampleCommandHandle = ep.CommandHandle[CommandId, int]
SampleProcessorFactory = ep.ProcessorFactory[CommandId, X]

R = int


class Cmd:
    @dataclasses.dataclass
    class Mult(_SampleCommand):
        cmdid = CommandId.MULT

        x: int
        y: int

        def exec(self, hcmd: SampleCommandHandle, cxt: X) -> R:
            return self.x * self.y * cxt

    @dataclasses.dataclass
    class Add(_SampleCommand):
        cmdid = CommandId.ADD

        x: int
        y: int

        def exec(self, hcmd: SampleCommandHandle, cxt: int) -> R:
            return self.x + self.y + cxt


def test_sample_context_simple():
    cxt = 5
    with ThreadPoolExecutor(1, thread_name_prefix="Sample") as executor:
        with SampleProcessorFactory(executor=executor, name="Sample", cxt=cxt) as sp:
            # the context manager starts the processor automatically
            assert not sp.paused()

            addran = 0

            def _assert_add(x: int, y: int, result: int):
                nonlocal addran
                assert result == x + y + cxt
                addran += 1

            multran = 0

            def _assert_mult(x: int, y: int, result: int):
                nonlocal multran
                assert result == x * y * cxt
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
