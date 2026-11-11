"""Local customer change-data-capture pipeline."""

from .pipeline import process_file

__all__ = ["process_file"]