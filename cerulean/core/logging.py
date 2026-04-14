"""
cerulean/core/logging.py
─────────────────────────────────────────────────────────────────────────────
Structured logging via structlog. Call configure_logging() once at startup.
Use get_logger(__name__) everywhere instead of stdlib logging directly.
"""

import logging
import sys

import structlog

from cerulean.core.config import get_settings


def configure_logging() -> None:
    """Configure structlog for the application. Call once at startup."""
    settings = get_settings()
    log_level = getattr(logging, settings.log_level.upper(), logging.INFO)

    shared_processors = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
    ]

    if settings.debug:
        # Human-readable in dev
        processors = shared_processors + [
            structlog.dev.ConsoleRenderer(colors=True),
        ]
    else:
        # JSON in prod
        processors = shared_processors + [
            structlog.processors.dict_tracebacks,
            structlog.processors.JSONRenderer(),
        ]

    # Write to both stdout and /tmp/cerulean.log
    _log_file = open("/tmp/cerulean.log", "a", buffering=1)  # line-buffered

    class _DualWriter:
        """Write to both stdout and a log file."""
        def __init__(self):
            self.stdout = sys.stdout
            self.logfile = _log_file
        def write(self, msg):
            self.stdout.write(msg)
            try:
                self.logfile.write(msg)
            except Exception:
                pass
        def flush(self):
            self.stdout.flush()
            try:
                self.logfile.flush()
            except Exception:
                pass

    dual = _DualWriter()

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(file=dual),
        cache_logger_on_first_use=True,
    )

    # Route stdlib logging through structlog
    logging.basicConfig(level=log_level, stream=dual, format="%(message)s")


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """Return a bound structlog logger for the given module name."""
    return structlog.get_logger(name)
