class CmdProcError(Exception):
    pass


class ConfigureWhileRunningError(CmdProcError):
    pass


class ResultCallbackError(CmdProcError):
    pass


class UnknownCmdError(CmdProcError):
    pass


class InvalidDataError(CmdProcError):
    pass
