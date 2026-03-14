"""
Structured logging configuration using structlog.

Provides:
- JSON logging for production
- Pretty console logging for development
- File rotation support
- Request context tracking
"""
import logging
import sys
from pathlib import Path
from typing import Optional

import structlog
from structlog.typing import Processor

from src.config import get_config


def setup_logging(level: Optional[str] = None) -> structlog.BoundLogger:
    """
    Configure structured logging.
    
    Args:
        level: Override log level (DEBUG, INFO, WARNING, ERROR)
        
    Returns:
        Configured logger instance
    """
    config = get_config()
    log_level = level or config.logging.level
    
    # Create logs directory if needed
    if config.logging.file.enabled:
        log_path = Path(config.logging.file.path)
        log_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Common processors
    shared_processors: list[Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
    ]
    
    if config.logging.format == "json":
        # JSON format for production
        processors = shared_processors + [
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ]
    else:
        # Pretty console format for development
        processors = shared_processors + [
            structlog.dev.ConsoleRenderer(colors=True),
        ]
    
    # Configure structlog
    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, log_level.upper())
        ),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )
    
    # Also configure standard library logging (for third-party libs)
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, log_level.upper()),
    )
    
    return get_logger()


def get_logger(name: Optional[str] = None) -> structlog.BoundLogger:
    """
    Get a logger instance.
    
    Args:
        name: Optional logger name for context
        
    Returns:
        Structured logger instance
    """
    logger = structlog.get_logger()
    if name:
        logger = logger.bind(component=name)
    return logger


class LogContext:
    """Context manager for adding temporary log context."""
    
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.token = None
    
    def __enter__(self):
        self.token = structlog.contextvars.bind_contextvars(**self.kwargs)
        return self
    
    def __exit__(self, *args):
        structlog.contextvars.unbind_contextvars(*self.kwargs.keys())
