"""Application-specific exceptions with user-facing messages."""


class MetroPTError(Exception):
    """Base class for expected application failures."""


class ConfigurationError(MetroPTError):
    """Raised when runtime configuration is invalid."""


class DataValidationError(MetroPTError):
    """Raised when an input row violates the MetroPT-3 schema."""


class RepositoryError(MetroPTError):
    """Raised when MongoDB cannot complete a repository operation."""
