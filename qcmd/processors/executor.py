import concurrent.futures as conc
import enum
import threading
from queue import PriorityQueue
from typing import ClassVar, Tuple, TypeVar, Union

import qcmd.core as core

I = TypeVar("I")
X = TypeVar("X")
R = TypeVar("R")


Command = core.Command[I, X, R]
CommandHandle = core.CommandHandle[I, R]


class _Control(enum.Enum):
    BREAK = enum.auto()


_ControlCommandHandle = core.CommandHandle[_Control, None]
_Entry = Union[Tuple[CommandHandle[I, R], Command[I, X, R]], Tuple[_ControlCommandHandle, None]]


class Processor(core.CommandProcessor[I, X]):
    def __init__(self, name: str, executor: conc.Executor, cxt: X = None):
        self._name = name
        self._cxt = cxt
        self._entry = 1
        self._q: PriorityQueue[_Entry] = PriorityQueue()
        self._qevent = threading.Event()
        self._qexecutor = executor
        self._qexecutor.submit(self._consume)

    def start(self) -> None:
        core.logevent("START", self)
        self._qevent.set()

    def pause(self) -> None:
        self._qevent.clear()

    def paused(self) -> bool:
        return not self._qevent.is_set()

    def join(self) -> None:
        core.logevent("JOIN", f"""{self} +{self._q.qsize()}+""")
        self._q.join()
        core.logevent("----", self)

    def halt(self) -> None:
        core.logevent("HALT", self)
        hcmd = _ControlCommandHandle(pri=0, entry=0, cmdid=_Control.BREAK)
        self._q.put((hcmd, None))
        if self.paused():
            self.start()

        core.logevent("XXXX", self)

    def send(self, cmd: Command, pri: int = 50, tags: core.Tags = ()) -> CommandHandle:
        hcmd = cmd.get_handle(pri, self._entry, tags, self._name)
        self._entry += 1
        self._q.put((hcmd, cmd))
        core.logevent("RCVD", hcmd)
        return hcmd

    def _consume(self) -> None:
        while self._qevent.wait():
            hcmd, cmd = self._q.get(block=True, timeout=None)

            if isinstance(hcmd.cmdid, _Control):
                core.logevent("CTRL", f"""{self} {cmd}""")
                if hcmd.cmdid == _Control.BREAK:
                    break

            core.logevent("EXEC", cmd)
            try:
                assert isinstance(cmd, core.Command)
                cmd(hcmd, self._cxt)
            except Exception as ex:
                core.logevent("ERRR", hcmd, ex.__cause__)
            finally:
                core.logevent("DONE", f"""{hcmd} +{self._q.qsize()}+""")
                self._q.task_done()

    def __repr__(self) -> str:
        return f"""<Processor '{self._name}' total_entries={self._entry - 1} at {id(self)}>"""


class ProcessorFactory(core.CommandProcessorFactory[I, X]):
    procname: ClassVar[str] = "Proc"

    def __init__(self, executor: conc.Executor, cxt: X):
        self._executor = executor
        super().__init__(cxt)

    def create(self, cxt: X) -> Processor[I, X]:
        return Processor(self.procname, self._executor, cxt)
