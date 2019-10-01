""" Fake DB API 2.0 for App engine """


class DatabaseError(Exception):
    pass


class IntegrityError(DatabaseError):
    pass


class NotSupportedError(Exception):
    pass


class CouldBeSupportedError(NotSupportedError):
    pass


class DataError(DatabaseError):
    pass


class OperationalError(DatabaseError):
    pass


class InternalError(DatabaseError):
    pass


class ProgrammingError(DatabaseError):
    pass


class InterfaceError(DatabaseError):
    pass


def Binary(val):
    return val


Error = DatabaseError
Warning = DatabaseError
