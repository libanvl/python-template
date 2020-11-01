from __future__ import annotations

import logging
import threading
import time
from abc import abstractmethod
from types import TracebackType
from typing import (
    Any,
    Callable,
    ClassVar,
    Container,
    ContextManager,
    Generic,
    Optional,
    Protocol,
    Type,
    TypeVar,
)

from qcmd.exceptions import CmdProcError, ResultCallbackError

I = TypeVar("I")
I.__doc__ = "CommandId: I"

X = TypeVar("X")
X.__doc__ = "Context: X"

R = TypeVar("R")
R.__doc__ = "Result: R"

Tags = Container[Any]

_logger = logging.getLogger(__name__)
_startime = time.time()


def logevent(evt: str, msg: Any, detail: Any = None) -> None:
    _logger.info(f"""[+{(time.time() - _startime):010.4f}s] {evt:5} {msg}""")
    if detail:
        _logger.info(f"""\t\t{detail}""")


class ErrorCallback(Protocol):
    def __call__(self, ex: Exception, tags: Tags = []) -> Optional[bool]:
        return True  # also raise


class _DefaultErrorCallback(ErrorCallback):
    pass


class CommandHandle(Generic[I, R]):
    onresult: Optional[Callable[[R, Tags], None]] = None
    onerror: ErrorCallback = _DefaultErrorCallback()

    def __init__(self, pri: int, entry: int, cmdid: I, tags: Tags = [], procname=None) -> None:
        self.pri = pri
        self.entry = entry
        self.cmdid = cmdid
        self.tags = tags
        self.procname = procname

    def then(self, onresult: Callable[[R, Tags], None]) -> CommandHandle[I, R]:
        self.onresult = onresult
        return self

    def or_err(self, onerror: Optional[ErrorCallback]) -> CommandHandle[I, R]:
        self.onerror = onerror or _DefaultErrorCallback()
        return self

    def __lt__(self, lhs: CommandHandle[I, R]) -> bool:
        return self.pri < lhs.pri if self.pri != lhs.pri else self.entry < lhs.entry

    def __repr__(self) -> str:
        return f"""{self.procname}::{self.cmdid} order={(self.pri, self.entry)} tags={self.tags} tid={threading.current_thread().name}"""


class Command(Generic[I, X, R]):
    cmdid: ClassVar[I]

    def __call__(self, hcmd: CommandHandle[I, R], cxt: X) -> None:
        try:
            result = self.exec(hcmd, cxt)
            if hcmd.onresult:
                try:
                    hcmd.onresult(result, hcmd.tags)
                except Exception as ex:
                    raise ResultCallbackError(ex) from ex
        except Exception as ex:
            if not hcmd.onerror or hcmd.onerror(CmdProcError(ex), hcmd.tags):
                raise CmdProcError(ex) from ex

    @classmethod
    def get_handle(
        cls, pri: int, entry: int, tags: Tags = [], procname: Optional[str] = None
    ) -> CommandHandle[I, R]:
        return CommandHandle(pri, entry, cls.cmdid, tags, procname)

    @abstractmethod
    def exec(self, hcmd: CommandHandle[I, R], cxt: X) -> R:
        raise NotImplementedError


class CommandProcessor(Protocol[I, X]):
    def start(self) -> None:
        """Starts processing the command queue."""

    def halt(self) -> None:
        """Halts the processing queue and cleans up resources.

        The processor should be considered unusable after halt() is called.
        """

    def pause(self) -> None:
        """Pauses processing.

        The processor can be restarted by calling start().
        """

    def paused(self) -> bool:
        """Whether the processor is paused.

        Returns:
            bool: True is the processor is paused, False otherwise.
        """

    def join(self) -> None:
        """Blocks until all currently queued commands are processed."""

    def send(self, cmd: Command[I, X, R], pri: int = 50, tags: Tags = ()) -> CommandHandle[I, R]:
        """Send a commands to the queue for processing

        Args:
            cmd (Command[I, R]): The command.
            pri (int, optional): The command priority. Lower priorities are processed first. Defaults to 50.
            tags (Tags, optional): A collection of tags for use by the consumer. Defaults to ().

        Returns:
            CommandHandle[I, R]: A handle that represents the command.
        """


class CommandProcessorFactory(ContextManager[CommandProcessor[I, X]]):
    instance: CommandProcessor[I, X]

    def __init__(self, cxt: X) -> None:
        self.instance = self.create(cxt)

    def __enter__(self) -> CommandProcessor[I, X]:
        self.instance.start()
        return self.instance

    def __exit__(
        self,
        __exc_type: Optional[Type[BaseException]],
        __exc_value: Optional[BaseException],
        __traceback: Optional[TracebackType],
    ) -> Optional[bool]:
        self.instance.halt()
        return None

    @abstractmethod
    def create(self, cxt: X) -> CommandProcessor[I, X]:
        raise NotImplementedError()
