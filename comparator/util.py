import abc
import sys


if sys.version_info >= (3, 4):  # pragma: no cover
    ABC = abc.ABC
else:  # pragma: no cover
    ABC = abc.ABCMeta(str('ABC'), (), {})

try:  # pragma: no cover
    from pathlib import Path
    Path().expanduser()
except (ImportError, AttributeError):  # pragma: no cover
    from pathlib2 import Path
