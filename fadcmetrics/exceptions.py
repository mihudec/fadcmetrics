class FortinetApiException(Exception):
    pass

class AuthenticationFailed(FortinetApiException):
    pass

class UnknownApiException(FortinetApiException):
    pass


class DuplicateEntry(FortinetApiException):
    pass

class EntryDoesNotExist(FortinetApiException):
    pass

class EntryNotFound(FortinetApiException):
    pass


class FadcMetricsException(Exception):
    pass


class HttpWriterException(FadcMetricsException):
    pass