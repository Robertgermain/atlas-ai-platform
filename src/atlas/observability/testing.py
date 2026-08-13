"""Testing seam for Atlas-owned structured logging (Slice 15A1 correction).

:func:`atlas.observability.logging.configure_logging` atomically replaces
*every* existing root-logger handler on each call (see its own docstring
for why) -- so a test harness's own generic root-level log capture (e.g.
attaching a handler to the root logger and relying on propagation, which
is how pytest's ``caplog`` fixture works) does not survive a call to
``configure_logging()`` made by the code under test.

:func:`capture_logs` is the sanctioned replacement. It attaches a
dedicated capturing handler *directly* to the named logger(s) being
tested, which receives every record emitted on that logger regardless of
whatever ``configure_logging()`` does to the root logger --
``logging.Logger.callHandlers`` always invokes handlers attached to the
logger itself before propagating a record upward, and removing the
capturing handler here never touches the root logger at all. Every
captured record is rendered through the exact same
:class:`~atlas.observability.logging.AtlasJSONFormatter` production uses,
so assertions against ``captured.rendered``/``captured.text``/
``captured.json(i)`` verify precisely what would reach stdout in
production -- this is a genuine testing seam, not a relaxation of the
production contract.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterator
from contextlib import contextmanager

from atlas.observability.logging import AtlasJSONFormatter


class CapturedLogs:
    """Records and Atlas-JSON-rendered lines captured by :func:`capture_logs`."""

    def __init__(self) -> None:
        self.records: list[logging.LogRecord] = []
        self.rendered: list[str] = []

    @property
    def text(self) -> str:
        """Every captured line, newline-joined (mirrors ``caplog.text``)."""
        return "\n".join(self.rendered)

    @property
    def events(self) -> list[str | None]:
        """The ``event`` structured field of each captured record, in order."""
        return [getattr(record, "event", None) for record in self.records]

    def json(self, index: int = 0) -> dict[str, object]:
        """Parse the ``index``-th captured line as the JSON object it is."""
        return dict(json.loads(self.rendered[index]))


class _CollectingHandler(logging.Handler):
    def __init__(self, sink: CapturedLogs, formatter: AtlasJSONFormatter) -> None:
        super().__init__(level=logging.NOTSET)
        self._sink = sink
        self._formatter = formatter

    def emit(self, record: logging.LogRecord) -> None:
        self._sink.records.append(record)
        self._sink.rendered.append(self._formatter.format(record))


@contextmanager
def capture_logs(*loggers: str | logging.Logger) -> Iterator[CapturedLogs]:
    """Capture every record emitted on the given logger(s), safely.

    Attaches a dedicated handler directly to each named logger (accepting
    either a logger name or a :class:`logging.Logger` instance) for the
    duration of the ``with`` block, temporarily lowering that logger's own
    level to :data:`logging.INFO` if it was unset or higher -- so an
    ``INFO``-level event is captured regardless of the ambient root-logger
    level, independent of whether :func:`~atlas.observability.logging.
    configure_logging` has run yet in this process. Restores the previous
    level and detaches the handler on exit either way.

    Renders synchronously at emit time (matching a real handler), so
    ambient :mod:`atlas.observability.context` correlation fields -- valid
    only for the duration of their own ``bind_context`` block -- are
    captured correctly rather than read back after the block has already
    exited.
    """
    sink = CapturedLogs()
    handler = _CollectingHandler(sink, AtlasJSONFormatter())
    resolved = [
        logging.getLogger(target) if isinstance(target, str) else target
        for target in loggers
    ]
    previous_levels = [target.level for target in resolved]
    for target in resolved:
        target.addHandler(handler)
        if target.level == logging.NOTSET or target.level > logging.INFO:
            target.setLevel(logging.INFO)
    try:
        yield sink
    finally:
        for target, level in zip(resolved, previous_levels, strict=True):
            target.removeHandler(handler)
            target.setLevel(level)
