# analytics.py

import time
import asyncio
import logging
from datetime import datetime
from functools import wraps
from typing import Callable, Any, Dict, List

from scripts.reports import save_html_report


class Analytics:
    def __init__(self, config: Dict[str, Any], console_logger, error_logger, analytics_logger):
        """
        Initializes the Analytics class with the configuration and loggers.
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
    ):
        """
        Writes the event to the self.events list,
        and does minimal logging to the console and analytics.log
        """
        # Save the event (for HTML report)
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

        # Minimalistic logging
        if success:
            # If the call is successful; we log it at DEBUG level for minimal spam
            msg = f"[A] {function_name} ({event_type}) took {duration:.3f}s (OK)."
            if duration > self.long_duration_threshold:
                msg += " [LONG!]"
            self.console_logger.debug(msg)
            self.analytics_logger.debug(msg)
        else:
            # If there is an error, display warning
            msg = f"[A] {function_name} ({event_type}) failed after {duration:.3f}s!"
            self.console_logger.warning(msg)
            self.analytics_logger.warning(msg)

    def log_decorator_overhead(self, function_name: str, overhead: float):
        self.decorator_overhead[function_name] = self.decorator_overhead.get(function_name, 0) + overhead


    def decorator(self, event_type: str):
        def decorator_function(func: Callable):
            if asyncio.iscoroutinefunction(func):
                @wraps(func)
                async def async_wrapper(*args, **kwargs):
                    func_name = func.__name__
                    decorator_start = time.time()
                    try:
                        function_start = time.time()
                        result = await func(*args, **kwargs)
                        function_end = time.time()
                        success = True
                        return result
                    except Exception as e:
                        function_end = time.time()
                        success = False
                        raise e
                    finally:
                        decorator_end = time.time()
                        function_duration = function_end - function_start
                        decorator_duration = decorator_end - decorator_start
                        overhead = decorator_duration - function_duration
                        self.log_decorator_overhead(func_name, overhead)
                        self.log_event(func_name, event_type, function_start, function_end, function_duration, success)
                return async_wrapper
            else:
                @wraps(func)
                def sync_wrapper(*args, **kwargs):
                    func_name = func.__name__
                    decorator_start = time.time()
                    try:
                        function_start = time.time()
                        result = func(*args, **kwargs)
                        function_end = time.time()
                        success = True
                        return result
                    except Exception as e:
                        function_end = time.time()
                        success = False
                        raise e
                    finally:
                        decorator_end = time.time()
                        function_duration = function_end - function_start
                        decorator_duration = decorator_end - decorator_start
                        overhead = decorator_duration - function_duration
                        self.log_decorator_overhead(func_name, overhead)
                        self.log_event(func_name, event_type, function_start, function_end, function_duration, success)
                return sync_wrapper
        return decorator_function


    def generate_reports(self):
        """
        Generates HTML reports based on the collected data, using an optimized approach 
        to reduce large file size (grouping short & successful calls).
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