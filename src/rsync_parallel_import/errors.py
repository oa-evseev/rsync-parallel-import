class ImporterError(Exception):
    """Base class for errors that should be shown without a traceback."""


class ConfigurationError(ImporterError):
    pass


class ManifestError(ImporterError):
    pass


class SourceChangedError(ImporterError):
    pass


class StateError(ImporterError):
    pass


class LockError(ImporterError):
    pass


class PrerequisiteError(ImporterError):
    pass


class TransferError(ImporterError):
    pass

