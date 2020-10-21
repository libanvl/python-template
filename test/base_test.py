import re
from typing import Any, Container

import pytest
from cmdq.base import Command, CommandHandle, _DefaultErrorCallback
from cmdq.exceptions import CmdProcError


class Test_DefaultErrorCallback:
    def test_returns_true(self):
        x = _DefaultErrorCallback()  # type:ignore reportPrivateUsage
        assert x(Exception())


class Test_CommandHandle:
    def test_then(self):
        x = CommandHandle[int, str](0, 0, 0)

        # default result callback is None
        assert x.onresult is None

        def _resultcb(res: str, tags: Container[Any]):
            return None

        y = x.then(_resultcb)

        # then returns same object
        assert y is x
        assert y.onresult is _resultcb

    def test_or_err(self):
        x = CommandHandle[int, str](0, 0, 0)

        def _errcb(ex: Exception, tags: Container[Any]):
            return None

        # default error callback
        assert isinstance(x.onerror, _DefaultErrorCallback)  # type:ignore reportPrivateUsage

        y = x.or_err(_errcb)

        # returns same object
        assert y is x
        assert y.onerror is _errcb

        y = y.or_err(None)

        # resets to default error callback
        assert isinstance(y.onerror, _DefaultErrorCallback)  # type:ignore reportPrivateUsage

    def test_lt(self):
        x = CommandHandle[int, int](0, 0, 0)
        y = CommandHandle[int, int](0, 0, 0)
        assert not x < y

        # entry breaks a priority tie
        y = CommandHandle[int, int](0, 1, 0)
        assert x < y

        # priority wins
        y = CommandHandle[int, int](1, 1, 0)
        assert x < y


class Test_Command:
    def test_cmdid_must_be_set(self):
        class _TestCommand(Command[int, int, int]):
            # cmdId class var not set in concrete set

            def exec(self, hcmd: CommandHandle[int, int], cxt: int) -> int:
                return 0

        x = _TestCommand()
        with pytest.raises(AttributeError, match=re.compile(".*cmdId.*")):
            x.get_handle(0, 0, [])

    def test_resultcallback(self):
        class _TestCommand(Command[int, None, int]):
            cmdId = 0

            def exec(self, hcmd: CommandHandle[int, int], cxt: None) -> int:
                return 1

        cmd = _TestCommand()
        hcmd = _TestCommand.get_handle(0, 0, [])

        # no result callback does not raise error
        cmd(hcmd, None)

        rcb_exec = False

        def _resultcb(res: int, tags: Container[Any]) -> int:
            nonlocal rcb_exec
            rcb_exec = True
            return 0

        hcmd.then(_resultcb)
        cmd(hcmd, None)

        # result callback is executed
        assert rcb_exec

        def _raisescb(res: int, tags: Container[Any]) -> int:
            raise Exception("test-value")

        hcmd.then(_raisescb)

        with pytest.raises(CmdProcError):
            cmd(hcmd, None)
