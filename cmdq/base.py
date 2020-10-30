from __future__ import annotations

import logging
import threading
import time
from abc import abstractclassmethod, abstractmethod
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

from cmdq.exceptions import CmdProcError, ResultCallbackError

Tcmdid = TypeVar("Tcmdid")
Tcxt = TypeVar("Tcxt")
Tres = TypeVar("Tres")

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


class CommandHandle(Generic[Tcmdid, Tres]):
    _ResultCallback = Callable[[Tres, Tags], Optional[Any]]

    onresult: Optional[_ResultCallback] = None
    onerror: ErrorCallback = _DefaultErrorCallback()

    def __init__(
        self, pri: int, entry: int, cmdid: Tcmdid, tags: Tags = [], procname=None
    ) -> None:
        self.pri = pri
        self.entry = entry
        self.cmdid = cmdid
        self.tags = tags
        self.procname = procname

    def then(self, onresult: _ResultCallback) -> CommandHandle[Tcmdid, Tres]:
        self.onresult = onresult
        return self

    def or_err(self, onerror: Optional[ErrorCallback]) -> CommandHandle[Tcmdid, Tres]:
        self.onerror = onerror or _DefaultErrorCallback()
        return self

    def __lt__(self, lhs: CommandHandle[Tcmdid, Tres]) -> bool:
        return self.pri < lhs.pri if self.pri != lhs.pri else self.entry < lhs.entry

    def __repr__(self) -> str:
        return f"""{self.procname}::{self.cmdid} order={(self.pri, self.entry)} tags={self.tags} tid={threading.current_thread().name}"""


class Command(Protocol[Tcmdid, Tcxt, Tres]):
    cmdId: ClassVar[Tcmdid]

    def __call__(self, hcmd: CommandHandle[Tcmdid, Tres], cxt: Tcxt) -> None:
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
    ) -> CommandHandle[Tcmdid, Tres]:
        return CommandHandle(pri, entry, cls.cmdId, tags, procname)

    @abstractmethod
    def exec(self, hcmd: CommandHandle[Tcmdid, Tres], cxt: Tcxt) -> Tres:
        raise NotImplementedError


class CommandProcessor(Protocol[Tcmdid, Tcxt]):
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

    def send(
        self, cmd: Command[Tcmdid, Tcxt, Tres], pri: int = 50, tags: Tags = ()
    ) -> CommandHandle[Tcmdid, Tres]:
        """Send a commands to the queued for processing

        Args:
            cmd (Command[Tcmdid, Tcxt, Any]): The command.
            pri (int, optional): The command priority. Lower priorities are processed first. Defaults to 50.
            tags (Tags, optional): A collection of tags for use by the consumer. Defaults to ().

        Returns:
            CommandHandle[Tcmdid, Any]: A handle that represents the command.
        """


class CommandProcessorHandle(ContextManager[CommandProcessor[Tcmdid, Tcxt]]):
    instance: CommandProcessor[Tcmdid, Tcxt]

    def __init__(self, cxt: Tcxt) -> None:
        self.instance = self.factory(cxt)

    def __enter__(self) -> CommandProcessor[Tcmdid, Tcxt]:
        self.instance.start()
        return self.instance

    def __exit__(
        self,
        __exc_type: Optional[Type[BaseException]],
        __exc_value: Optional[BaseException],
        __traceback: Optional[TracebackType],
    ) -> Optional[bool]:
        self.instance.halt()

    @abstractclassmethod
    def factory(cls, cxt: Tcxt) -> CommandProcessor[Tcmdid, Tcxt]:
        raise NotImplementedError()
