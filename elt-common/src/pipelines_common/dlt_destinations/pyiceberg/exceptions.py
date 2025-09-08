from functools import wraps
from typing import (
    Any,
)
from dlt.common.destination.exceptions import (
    DestinationUndefinedEntity,
    DestinationTransientException,
    DestinationTerminalException,
)
from dlt.common.destination.client import JobClientBase
from dlt.common.typing import TFun

from pyiceberg.exceptions import (
    NoSuchTableError,
    NoSuchIdentifierError,
    RESTError,
)


def pyiceberg_error(f: TFun) -> TFun:
    @wraps(f)
    def _wrap(self: JobClientBase, *args: Any, **kwargs: Any) -> Any:
        try:
            return f(self, *args, **kwargs)
        except (
            NoSuchIdentifierError,
            NoSuchTableError,
        ) as status_ex:
            raise DestinationUndefinedEntity(status_ex) from status_ex
        except RESTError as e:
            raise DestinationTransientException(e) from e
        except Exception as status_ex:
            raise DestinationTerminalException(status_ex) from status_ex

    return _wrap  # type: ignore[return-value]
