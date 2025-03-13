#!/usr/bin/env python3

"""
Analytics Module

Provides decorators to measure function execution time,
track success/failure, and generate an HTML report for analytics.
"""

import asyncio
import time

from datetime import datetime
from functools import wraps
from typing import Any, Callable, Dict, List

from utils.reports import save_html_report

class Analytics:
    """
    Collects and logs analytics events for function calls.
    """

    def __init__(self, config: Dict[str, Any], console_logger, error_logger, analytics_logger):
        """
        Initializes the Analytics class with the configuration and loggers.

        :param config: Configuration dictionary.
        :param console_logger: Logger for console messages.
        :param error_logger: Logger for error messages.
        :param analytics_logger: Logger for analytics messages.
        """
        self.events: List[Dict[str, Any]] = []
        self.call_counts: Dict[str, int] = {}
        self.success_counts: Dict[str, int] = {}
        self.decorator_overhead: Dict[str, float] = {}
        self.config = config
        self.console_logger = console_logger
        self.error_logger = error_logger
        self.analytics_logger = analytics_logger

        # Duration threshold after which the function is considered “long”
        self.long_duration_threshold = config.get("analytics", {}).get("duration_thresholds", {}).get("long_max", 10)

    def log_event(
        self,
        function_name: str,
        event_type: str,
        start_time: float,
        end_time: float,
        duration: float,
        success: bool
    ) -> None:
        """
        Write the event to self.events and perform minimal logging.

        :param function_name: Name of the function being called.
        :param event_type: Type/description of the event.
        :param start_time: Epoch start time.
        :param end_time: Epoch end time.
        :param duration: Duration (seconds).
        :param success: Whether function succeeded or not.
        """
        self.events.append({
            "Function": function_name,
            "Event Type": event_type,
            "Start Time": datetime.fromtimestamp(start_time).strftime("%Y-%m-%d %H:%M:%S"),
            "End Time": datetime.fromtimestamp(end_time).strftime("%Y-%m-%d %H:%M:%S"),
            "Duration (s)": round(duration, 4),
            "Success": success
        })

        self.call_counts[function_name] = self.call_counts.get(function_name, 0) + 1
        if success:
            self.success_counts[function_name] = self.success_counts.get(function_name, 0) + 1
            msg = f"[A] {function_name} ({event_type}) took {duration:.3f}s (OK)."
            if duration > self.long_duration_threshold:
                msg += " [LONG!]"
            self.console_logger.debug(msg)
            self.analytics_logger.debug(msg)
        else:
            msg = f"[A] {function_name} ({event_type}) failed after {duration:.3f}s!"
            self.console_logger.warning(msg)
            self.analytics_logger.warning(msg)

    def log_decorator_overhead(self, function_name: str, overhead: float) -> None:
        """
        Accumulate overhead time introduced by the decorator.
        """
        self.decorator_overhead[function_name] = (
            self.decorator_overhead.get(function_name, 0.0) + overhead
        )

    def decorator(self, event_type: str) -> Callable:
        """
        Return a decorator function that measures execution time and logs events.

        :param event_type: The type of event to log.
        """
        def decorator_function(func: Callable) -> Callable:
            if asyncio.iscoroutinefunction(func):
                @wraps(func)
                async def async_wrapper(*args, **kwargs):
                    func_name = func.__name__
                    decorator_start = time.time()
                    function_start = time.time()
                    function_end = function_start
                    success = False
                    try:
                        result = await func(*args, **kwargs)
                        success = True
                        return result
                    except Exception as e:
                        function_end = time.time()
                        raise e
                    finally:
                        decorator_end = time.time()
                        function_duration = function_end - function_start
                        decorator_duration = decorator_end - decorator_start
                        overhead = decorator_duration - function_duration
                        self.log_decorator_overhead(func_name, overhead)
                        self.log_event(func_name, event_type, function_start, function_end, function_duration, success)
                        self.analytics_logger.info("[Analytics] Async function execution completed.", extra={"section_end": True})
                return async_wrapper
            else:
                @wraps(func)
                def sync_wrapper(*args, **kwargs):
                    func_name = func.__name__
                    decorator_start = time.time()
                    function_start = time.time()
                    function_end = function_start
                    success = False
                    try:
                        result = func(*args, **kwargs)
                        success = True
                        return result
                    except Exception as e:
                        function_end = time.time()
                        raise e
                    finally:
                        function_end = time.time()
                        decorator_end = time.time()
                        function_duration = function_end - function_start
                        decorator_duration = decorator_end - decorator_start
                        overhead = decorator_duration - function_duration
                        self.log_decorator_overhead(func_name, overhead)
                        self.log_event(func_name, event_type, function_start, function_end, function_duration, success)
                        self.analytics_logger.info("SECTION_END", extra={"section_end": True})
                return sync_wrapper
        return decorator_function

    def generate_reports(self) -> None:
        """
        Generate an optimized HTML report, grouping short & successful calls
        to reduce file size.
        """
        try:
            save_html_report(
                self.events,
                self.call_counts,
                self.success_counts,
                self.decorator_overhead,
                self.config,
                self.console_logger,
                self.error_logger,
                group_successful_short_calls=True
            )
            self.analytics_logger.info("[Analytics] HTML report generated successfully (optimized).")
        except Exception as e:
            self.error_logger.error(f"Failed to generate HTML report: {e}")
            self.analytics_logger.error(f"Failed to generate HTML report: {e}")