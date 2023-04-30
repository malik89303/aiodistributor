def int_or_str(value: str | int) -> int:
    try:
        return int(value)
    except ValueError:
        return value  # type: ignore


__version__ = '0.0.2.1'
VERSION = tuple(map(int_or_str, __version__.split('.')))
