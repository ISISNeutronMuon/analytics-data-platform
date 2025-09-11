from collections.abc import Sequence
import logging


def _log_filter_factory(keep_records_from: Sequence[str]):
    class _FilterUnwantedRecords:
        def filter(self, record):
            for keep in keep_records_from:
                if keep in record.name:
                    return True

            return False

    return _FilterUnwantedRecords()


def configure_logging(root_level: int | str, keep_records_from: Sequence[str] | None = None):
    """Configure logging to log at the given level

    If a module name is given then configure logger to
    only allow messages from that module
    :param root_level: The log level for the root logger
    :keep_records_from: A list of string patterns giving module names whose log records should be kept.
                        The check is a simple is pattern in 'record.name'
    """
    logging.basicConfig(level=root_level)
    if keep_records_from is not None:
        filter = _log_filter_factory(keep_records_from)
        for handler in logging.getLogger().handlers:
            handler.addFilter(filter)
