#!/usr/bin/env python3
"""
Analytics Module

This module provides a class for collecting and logging analytics events for function calls.
It includes a decorator to measure execution time and log events.

    1. Create an instance of the Analytics class.
    2. Decorate functions with the @analytics_instance.decorator("Event Type") decorator.
    3. Call the decorated functions as usual.
"""

import asyncio
import time

from datetime import datetime
from functools import wraps
from typing import Any, Callable, Dict, List

from utils.reports import save_html_report

class Analytics:
    """
    Collects and logs analytics events for function calls. Includes a decorator to measure execution time.

    :param config: Configuration dictionary.
    :param console_logger: Logger for console messages.
    :param error_logger: Logger for error messages.
    :param analytics_logger: Logger for analytics messages.
    """
    # Static Counter to Identify specimens
    _instances = 0
    
    def __init__(self, config: Dict[str, Any], console_logger, error_logger, analytics_logger):
        """
        Initializes the Analytics class with the configuration and loggers.

        :param config: Configuration dictionary.
        :param console_logger: Logger for console messages.
        :param error_logger: Logger for error messages.
        :param analytics_logger: Logger for analytics messages.
        """
        # Unique instance identifier
        Analytics._instances += 1
        self.instance_id = Analytics._instances

        self.events: List[Dict[str, Any]] = []
        self.call_counts: Dict[str, int] = {}
        self.success_counts: Dict[str, int] = {}
        self.decorator_overhead: Dict[str, float] = {}
        self.config = config
        self.console_logger = console_logger
        self.error_logger = error_logger
        self.analytics_logger = analytics_logger

        # Duration threshold after which the function is considered "long"
        self.long_duration_threshold = config.get("analytics", {}).get("duration_thresholds", {}).get("long_max", 10)
        
        self.console_logger.debug(f"Analytics instance #{self.instance_id} initialized")
    
    def merge_with(self, other: 'Analytics') -> None:
        """
        Merge analytics data from another instance into this one.

        :param other: Another instance of the Analytics class.
        """
        if other is self:
            return
            
        self.console_logger.debug(f"Merging analytics from instance #{other.instance_id} to #{self.instance_id}")
        
        # Merge events
        self.events.extend(other.events)
        
        # Merge counters
        for fn, count in other.call_counts.items():
            self.call_counts[fn] = self.call_counts.get(fn, 0) + count
        
        for fn, count in other.success_counts.items():
            self.success_counts[fn] = self.success_counts.get(fn, 0) + count
            
        for fn, overhead in other.decorator_overhead.items():
            self.decorator_overhead[fn] = self.decorator_overhead.get(fn, 0) + overhead
            
        # Cleaning data in another instance
        other.events.clear()
        other.call_counts.clear()
        other.success_counts.clear()
        other.decorator_overhead.clear()

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
        Log an event for a function call.

        :param function_name: Name of the function.
        :param event_type: Type of event (e.g., "API call", "Database query").
        :param start_time: Start time of the function call.
        :param end_time: End time of the function call.
        :param duration: Duration of the function call in seconds.
        :param success: Whether the function call was successful.
        """
        # Log event details
        self.console_logger.debug(f"[A#{self.instance_id}] Adding event for {function_name}: success={success}, duration={duration:.3f}s")
        
        # Add to events list
        self.events.append({
            "Function": function_name,
            "Event Type": event_type,
            "Start Time": datetime.fromtimestamp(start_time).strftime("%Y-%m-%d %H:%M:%S"),
            "End Time": datetime.fromtimestamp(end_time).strftime("%Y-%m-%d %H:%M:%S"),
            "Duration (s)": round(duration, 4),
            "Success": success
        })

        # Update call counts
        self.call_counts[function_name] = self.call_counts.get(function_name, 0) + 1
        
        # Update success counts and log
        if success:
            self.success_counts[function_name] = self.success_counts.get(function_name, 0) + 1
            msg = f"[A#{self.instance_id}] {function_name} ({event_type}) took {duration:.3f}s (OK)."
            if duration > self.long_duration_threshold:
                msg += " [LONG!]"
            self.console_logger.debug(msg)
            self.analytics_logger.debug(msg)
        else:
            msg = f"[A#{self.instance_id}] {function_name} ({event_type}) failed after {duration:.3f}s!"
            self.console_logger.warning(msg)
            self.analytics_logger.warning(msg)

    def log_decorator_overhead(self, function_name: str, overhead: float) -> None:
        """
        Log decorator overhead time.
        
        :param function_name: Name of the decorated function.
        :param overhead: Overhead time introduced by the decorator in seconds.
        """
        # Accumulate overhead time introduced by the decorator
        self.decorator_overhead[function_name] = (
            self.decorator_overhead.get(function_name, 0.0) + overhead
        )

    def decorator(self, event_type: str) -> Callable:
        """
        Returns a decorator function that measures execution time and logs events.

        :param event_type: Type of event to log (e.g., "API call", "Database query").
        :return: Decorator function.
        """
        def decorator_function(func: Callable) -> Callable:
            if asyncio.iscoroutinefunction(func):
                @wraps(func)
                async def async_wrapper(*args, **kwargs):
                    func_name = func.__name__
                    decorator_start = time.time()
                    function_start = time.time()
                    success = False
                    try:
                        result = await func(*args, **kwargs)
                        success = True
                        return result
                    except Exception as e:
                        # Log error but maintain the exception flow
                        self.error_logger.error(f"Exception in {func_name}: {e}")
                        raise e
                    finally:
                        # Update function_end in finally to ensure correct timing
                        function_end = time.time()
                        decorator_end = time.time()
                        function_duration = function_end - function_start
                        overhead = decorator_end - decorator_start - function_duration
                        self.log_decorator_overhead(func_name, overhead)
                        self.log_event(func_name, event_type, function_start, function_end, function_duration, success)
                        self.analytics_logger.debug(f"[A#{self.instance_id}] Async function {func_name} completed.", extra={"section_end": True})
                return async_wrapper
            else:
                @wraps(func)
                def sync_wrapper(*args, **kwargs):
                    func_name = func.__name__
                    decorator_start = time.time()
                    function_start = time.time()
                    success = False
                    try:
                        result = func(*args, **kwargs)
                        success = True
                        return result
                    except Exception as e:
                        # Log error but maintain the exception flow
                        self.error_logger.error(f"Exception in {func_name}: {e}")
                        raise e
                    finally:
                        function_end = time.time()
                        decorator_end = time.time()
                        function_duration = function_end - function_start
                        overhead = decorator_end - decorator_start - function_duration
                        self.log_decorator_overhead(func_name, overhead)
                        self.log_event(func_name, event_type, function_start, function_end, function_duration, success)
                        self.analytics_logger.debug(f"[A#{self.instance_id}] Function {func_name} completed.", extra={"section_end": True})
                return sync_wrapper
        return decorator_function

    def generate_reports(self) -> None:
        """
        Generate an optimized HTML report, grouping short & successful calls
        to reduce file size.

        :return: None
        """
        try:
            event_count = len(self.events)
            function_count = len(self.call_counts)
            
            self.console_logger.info(f"[A#{self.instance_id}] Generating HTML report with {event_count} events, {function_count} functions")
            
            if event_count == 0 and function_count == 0:
                self.console_logger.warning("No analytics data to generate report. Check if decorators are working correctly.")
            
            from utils.reports import save_html_report
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
            self.analytics_logger.info(f"[A#{self.instance_id}] HTML report generated successfully (optimized).")
        except Exception as e:
            self.error_logger.error(f"Failed to generate HTML report: {e}", exc_info=True)
            self.analytics_logger.error(f"Failed to generate HTML report: {e}")