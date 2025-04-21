#!/usr/bin/env python3
"""
Analytics Module

Provides a comprehensive performance tracking and analysis system for Python applications.
Uses decorators to automatically measure execution time, success rates, and execution patterns.

Features:
    - Function performance tracking (sync and async)
    - Success/failure monitoring
    - Performance categorization (fast/medium/slow)
    - HTML report generation
    - Memory management for long-running applications
    - Statistical aggregation and filtering
    - Decorator factory for tracking instance methods

Usage:
    1. Create an instance of the Analytics class
    2. Decorate functions with @analytics.track("Event Type")
    3. Decorate instance methods with @Analytics.track_instance_method("Event Type")
    4. Access statistics via analytics.get_stats()
    5. Generate HTML reports with analytics.generate_reports()
    6. Get summaries with analytics.log_summary()
    7. Manage memory with analytics.clear_old_events()
    8. Merge data from multiple instances with analytics.merge_with()

Example:
    ```python
    from utils.analytics import Analytics
    import logging

    # Initialize with configuration and loggers (assuming config, loggers are defined)
    # config, console_logger, error_logger, analytics_logger = ...
    # For this example, using dummy loggers:
    console_logger = logging.getLogger('console')
    error_logger = logging.getLogger('error')
    analytics_logger = logging.getLogger('analytics')
    for logger in [console_logger, error_logger, analytics_logger]:
        if not logger.handlers:
            logger.addHandler(logging.StreamHandler())
            logger.setLevel(logging.DEBUG)

    analytics = Analytics({}, console_logger, error_logger, analytics_logger)

    # Track function performance
    @analytics.track("API Call")
    async def fetch_data(url):
        # Your code here
        await asyncio.sleep(0.1) # Simulate async work
        return "some data"

    # Track instance method performance
    class MyService:
        def __init__(self, analytics_instance: Analytics):
            self.analytics = analytics_instance
            self.error_logger = logging.getLogger('my_service_error')
            if not self.error_logger.handlers:
                 self.error_logger.addHandler(logging.StreamHandler())
                 self.error_logger.setLevel(logging.WARNING)


        @Analytics.track_instance_method("Service Method")
        async def process_item(self, item):
            # Your code here
            await asyncio.sleep(0.05) # Simulate async work
            print(f"Processing item: {item}")
            if "error" in item:
                 raise ValueError("Simulated processing error")


    # Example Usage
    async def run_example():
        await fetch_data("[http://example.com/api](http://example.com/api)")
        service_instance = MyService(analytics)
        await service_instance.process_item("item1")
        try:
             await service_instance.process_item("item_with_error")
        except ValueError as e:
             print(f"Caught expected error: {e}")

        # Get statistics for specific functions
        stats = analytics.get_stats() # Get stats for all tracked functions
        print("\nAnalytics Stats:")
        for func_name, func_stats in stats['functions'].items():
            print(f"  {func_name}: Calls={func_stats['total_calls']},
            Success Rate={func_stats['success_rate']:.1f}%, Avg Duration={func_stats['avg_duration']:.3f}s")

        # Generate HTML reports (requires utils.reports.save_html_report)
        # analytics.generate_reports()

        # Print summary to logs
        analytics.log_summary()

    # import asyncio
    # asyncio.run(run_example())
    ```

Memory Management:
    The Analytics class includes safeguards against unbounded memory growth:
    - Configurable max_events limit with automatic pruning
    - Manual cleanup of old events with clear_old_events()
    - Support for merging and consolidating data from multiple instances
"""

import asyncio
import gc
import logging
import sys
import time

from datetime import datetime, timedelta
from functools import wraps
from typing import Any, Callable, Dict, List, Union

# Assuming utils.reports.save_html_report exists or is a placeholder
# In a real scenario, you'd need a concrete implementation for reports.
try:
    from utils.reports import save_html_report
except ImportError:
    print("Warning: utils.reports.save_html_report not found. Report generation will be skipped.", flush=True)

    # Provide a dummy function if the import fails
    def save_html_report(*args, **kwargs):
        print("Report generation skipped: save_html_report function not available.", flush=True)


class Analytics:
    """
    Tracks function performance, success rates, and execution patterns.

    Provides decorators for tracking sync/async functions, aggregated statistics,
    and HTML report generation. Includes memory management features to prevent
    unbounded growth in long-running applications.
    """

    # Class-level counter for instance IDs
    _instances = 0

    def __init__(
        self,
        config: Dict[str, Any],
        console_logger: logging.Logger,
        error_logger: logging.Logger,
        analytics_logger: logging.Logger,
        max_events: int = None,
    ):
        """
        Initialize the Analytics tracker.
        """
        # Create unique instance identifier
        Analytics._instances += 1
        self.instance_id = Analytics._instances

        # Data storage
        self.events: List[Dict[str, Any]] = []
        self.call_counts: Dict[str, int] = {}
        self.success_counts: Dict[str, int] = {}
        self.decorator_overhead: Dict[str, float] = {}  # Simplified overhead for now

        # Configuration and loggers
        self.config = config
        self.console_logger = console_logger
        self.error_logger = error_logger
        self.analytics_logger = analytics_logger

        # Memory management settings
        self.max_events = max_events or config.get("analytics", {}).get("max_events", 10000)

        # Load thresholds from config
        thresholds = config.get("analytics", {}).get("duration_thresholds", {})
        # Use .get() with default values for safety
        self.short_max = thresholds.get("short_max", 2)
        self.medium_max = thresholds.get("medium_max", 5)
        self.long_max = thresholds.get("long_max", 10)

        # Time format settings
        self.time_format = config.get("analytics", {}).get("time_format", "%Y-%m-%d %H:%M:%S")
        self.compact_time = config.get("analytics", {}).get("compact_time", False)

        # Log initialization
        # Ensure loggers are not None before using
        if self.console_logger:
            self.console_logger.debug(f"📊 Analytics #{self.instance_id} initialized")

    def track(self, event_type: str) -> Callable:
        """
        Decorator factory that tracks function performance and success.
        This is for decorating regular functions (not methods).
        """

        def decorator_function(func: Callable) -> Callable:
            if asyncio.iscoroutinefunction(func):
                # Use self's async wrapper factory
                return self._create_async_wrapper(func, event_type)
            else:
                # Use self's sync wrapper factory
                return self._create_sync_wrapper(func, event_type)

        return decorator_function

    # --- Classmethod decorator factory for instance methods ---
    @classmethod
    def track_instance_method(cls, event_type: str) -> Callable:
        """
        Decorator factory for tracking instance method performance and success.
        This is applied as @Analytics.track_instance_method("Event Type")
        """

        def decorator(func: Callable) -> Callable:
            # 'func' is the method being decorated (e.g., MusicUpdater.fetch_tracks_async)
            # It expects 'self' (the instance) as its first argument when called.

            # Check if the original function is async
            is_async = asyncio.iscoroutinefunction(func)

            @wraps(func)  # Preserve original method's metadata
            async def async_wrapper(self, *args, **kwargs):
                # 'self' here IS the instance of the class the method belongs to (e.g., MusicUpdater)
                # We can now access self.analytics (which is an Analytics instance)
                # and self.error_logger (which is a logger instance).

                # Ensure the instance has an 'analytics' attribute which is an Analytics instance
                analytics_instance = getattr(self, 'analytics', None)
                if not isinstance(analytics_instance, Analytics):
                    # Fallback if analytics is not available or not the correct type
                    # Use a simple logger or print
                    logger_fallback = getattr(self, 'error_logger', None) or logging.getLogger(__name__)
                    logger_fallback.error(
                        f"Analytics instance not found on {self.__class__.__name__} instance for method {func.__name__}. Tracking skipped."
                    )
                    # Call the original method without tracking
                    # Need to await if the original function is async
                    return await func(self, *args, **kwargs) if is_async else func(self, *args, **kwargs)

                error_logger_instance = getattr(self, 'error_logger', None) or logging.getLogger(
                    __name__
                )  # Get error logger from 'self' or use fallback

                start_time = time.time()
                success = False
                try:
                    # Call the original method 'func' with 'self' and arguments
                    result = await func(self, *args, **kwargs) if is_async else func(self, *args, **kwargs)
                    success = True
                    return result
                except Exception as e:
                    # Log error using the instance's error logger
                    if error_logger_instance:
                        error_logger_instance.error(f"❌ {func.__name__}: {str(e)}")
                    else:
                        print(f"ERROR: ❌ {func.__name__}: {str(e)}", file=sys.stderr)
                    raise  # Re-raise the exception
                finally:
                    end_time = time.time()
                    duration = end_time - start_time
                    # Simplified overhead for now
                    overhead = 0.0
                    # Call the recording logic on the analytics instance
                    # We can call the private method directly as it's an internal helper
                    # Ensure analytics_instance exists before calling
                    if analytics_instance:
                        analytics_instance._record_function_call(func.__name__, event_type, start_time, end_time, duration, success, overhead)

            @wraps(func)
            def sync_wrapper(self, *args, **kwargs):
                # 'self' here IS the instance of the class the method belongs to (e.g., MusicUpdater)
                # We can now access self.analytics (which is an Analytics instance)
                # and self.error_logger (which is a logger instance).

                # Ensure the instance has an 'analytics' attribute which is an Analytics instance
                analytics_instance = getattr(self, 'analytics', None)
                if not isinstance(analytics_instance, Analytics):
                    # Fallback if analytics is not available or not the correct type
                    # Use a simple logger or print
                    logger_fallback = getattr(self, 'error_logger', None) or logging.getLogger(__name__)
                    logger_fallback.error(
                        f"Analytics instance not found on {self.__class__.__name__} instance for method {func.__name__}. Tracking skipped."
                    )
                    # Call the original method without tracking
                    return func(self, *args, **kwargs)

                error_logger_instance = getattr(self, 'error_logger', None) or logging.getLogger(
                    __name__
                )  # Get error logger from 'self' or use fallback

                start_time = time.time()
                success = False
                try:
                    # Call the original method 'func' with 'self' and arguments
                    result = func(self, *args, **kwargs)
                    success = True
                    return result
                except Exception as e:
                    # Log error using the instance's error logger
                    if error_logger_instance:
                        error_logger_instance.error(f"❌ {func.__name__}: {str(e)}")
                    else:
                        print(f"ERROR: ❌ {func.__name__}: {str(e)}", file=sys.stderr)
                    raise  # Re-raise the exception
                finally:
                    end_time = time.time()
                    duration = end_time - start_time
                    # Simplified overhead for now
                    overhead = 0.0
                    # Call the recording logic on the analytics instance
                    # We can call the private method directly as it's an internal helper
                    # Ensure analytics_instance exists before calling
                    if analytics_instance:
                        analytics_instance._record_function_call(func.__name__, event_type, start_time, end_time, duration, success, overhead)

            # Return the appropriate wrapper based on the original function type
            return async_wrapper if is_async else sync_wrapper

        return decorator  # Return the decorator function

    def decorator(self, event_type: str) -> Callable:
        """
        Legacy name for the track method. Provides same functionality.
        """
        return self.track(event_type)  # Just call the track method

    def _create_sync_wrapper(self, func: Callable, event_type: str) -> Callable:
        """
        Creates a synchronous wrapper for tracking function performance.
        Used by self.track for regular sync functions.
        """

        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            func_name = func.__name__
            # Note: Decorator overhead timing is simplified here compared to original
            # decorator_start = time.time()
            function_start = time.time()
            success = False
            try:
                result = func(*args, **kwargs)
                success = True
                return result
            except Exception as e:
                # Use the instance's error logger (self here is the Analytics instance)
                if self.error_logger:
                    self.error_logger.error(f"❌ {func_name}: {str(e)}")
                else:
                    print(f"ERROR: ❌ {func_name}: {str(e)}", file=sys.stderr)
                raise
            finally:
                function_end = time.time()
                # decorator_end = time.time()
                duration = function_end - function_start
                # overhead = decorator_end - decorator_start - duration # Simplified overhead
                overhead = 0.0  # Simplified overhead
                self._record_function_call(func_name, event_type, function_start, function_end, duration, success, overhead)

        return sync_wrapper

    def _create_async_wrapper(self, func: Callable, event_type: str) -> Callable:
        """
        Creates an asynchronous wrapper for tracking function performance.
        Used by self.track for regular async functions.
        """

        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            func_name = func.__name__
            # Note: Decorator overhead timing is simplified here compared to original
            # decorator_start = time.time()
            function_start = time.time()
            success = False
            try:
                result = await func(*args, **kwargs)
                success = True
                return result
            except Exception as e:
                # Use the instance's error logger (self here is the Analytics instance)
                if self.error_logger:
                    self.error_logger.error(f"❌ {func_name}: {str(e)}")
                else:
                    print(f"ERROR: ❌ {func_name}: {str(e)}", file=sys.stderr)
                raise
            finally:
                function_end = time.time()
                # decorator_end = time.time()
                duration = function_end - function_start
                # overhead = decorator_end - decorator_start - duration # Simplified overhead
                overhead = 0.0  # Simplified overhead
                self._record_function_call(func_name, event_type, function_start, function_end, duration, success, overhead)

        return async_wrapper

    def _record_function_call(
        self,
        func_name: str,
        event_type: str,
        start_time: float,
        end_time: float,
        duration: float,
        success: bool,
        overhead: float,  # Keep overhead parameter even if simplified
    ) -> None:
        """
        Records performance data for a function call.
        """
        # Manage memory by removing oldest events if needed
        if self.max_events > 0 and len(self.events) >= self.max_events:
            # Remove oldest 10% of events when limit is reached
            remove_count = max(1, int(self.max_events * 0.1))
            self.events = self.events[remove_count:]
            if remove_count > 0:  # Only log if pruning happens
                if self.console_logger:
                    self.console_logger.debug(f"📊 Pruned {remove_count} oldest events")

        # Format timestamp based on configuration
        try:
            start_dt = datetime.fromtimestamp(start_time)
            end_dt = datetime.fromtimestamp(end_time)
            if self.compact_time:
                start_str = start_dt.strftime("%H:%M:%S")
                end_str = end_dt.strftime("%H:%M:%S")
            else:
                start_str = start_dt.strftime(self.time_format)
                end_str = end_dt.strftime(self.time_format)
        except Exception as e:
            # Handle potential errors during timestamp formatting
            if self.error_logger:
                self.error_logger.error(f"Error formatting timestamp: {e}")
            else:
                print(f"ERROR: Error formatting timestamp: {e}", file=sys.stderr)
            start_str = "N/A"
            end_str = "N/A"

        # Add to events list
        self.events.append(
            {
                "Function": func_name,
                "Event Type": event_type,
                "Start Time": start_str,
                "End Time": end_str,
                "Duration (s)": round(duration, 4),
                "Success": success,
            }
        )

        # Update counters - use get() with default 0 for safety
        self.call_counts[func_name] = self.call_counts.get(func_name, 0) + 1
        if success:
            self.success_counts[func_name] = self.success_counts.get(func_name, 0) + 1

        # Track decorator overhead (simplified) - use get() with default 0.0 for safety
        self.decorator_overhead[func_name] = self.decorator_overhead.get(func_name, 0.0) + overhead

        # Log the outcome with appropriate duration category and symbol
        status = "✅" if success else "❌"
        duration_symbol = self._get_duration_symbol(duration)

        log_message = f"{status} {duration_symbol} {func_name} ({event_type}) took {duration:.3f}s"

        # Log to analytics logger and console logger - check if loggers exist
        if success:
            log_level = "info" if duration > self.long_max else "debug"
            if self.analytics_logger:
                getattr(self.analytics_logger, log_level)(log_message)
            if self.console_logger:
                getattr(self.console_logger, log_level)(log_message)

        else:  # Log failures as warnings
            if self.analytics_logger:
                self.analytics_logger.warning(log_message)
            if self.console_logger:
                self.console_logger.warning(log_message)

    def _get_duration_symbol(self, duration: float) -> str:
        """
        Returns a symbol representing the duration category.
        """
        if duration <= self.short_max:
            return "⚡"  # Fast
        elif duration <= self.medium_max:
            return "⏱️"  # Medium
        else:
            return "🐢"  # Slow

    def merge_with(self, other: 'Analytics') -> None:
        """
        Merge analytics data from another instance.
        """
        if other is self:
            return

        if self.console_logger:
            self.console_logger.debug(f"📊 Merging data from Analytics #{other.instance_id}")

        # Merge events with memory limit check
        remaining_capacity = self.max_events - len(self.events) if self.max_events > 0 else float('inf')
        events_to_add = other.events[: int(remaining_capacity)] if remaining_capacity < len(other.events) else other.events
        self.events.extend(events_to_add)

        # Merge counters - use get() with default 0 or 0.0 for safety
        for fn, count in other.call_counts.items():
            self.call_counts[fn] = self.call_counts.get(fn, 0) + count

        for fn, count in other.success_counts.items():
            self.success_counts[fn] = self.success_counts.get(fn, 0) + count

        for fn, overhead in other.decorator_overhead.items():
            self.decorator_overhead[fn] = self.decorator_overhead.get(fn, 0.0) + overhead

        # Clean other instance data
        other.events.clear()
        other.call_counts.clear()
        other.success_counts.clear()
        other.decorator_overhead.clear()

    def get_stats(self, function_filter: Union[str, List[str]] = None) -> Dict[str, Any]:
        """
        Get summary statistics for tracked functions.
        Returns a dictionary including overall stats and per-function stats.
        """
        # Ensure events list is processed even if filter is None
        if function_filter:
            if isinstance(function_filter, str):
                function_names = {function_filter}
            else:
                function_names = set(function_filter)

            # Filter events based on provided function names
            filtered_events = [e for e in self.events if e.get("Function") in function_names]
            # Filter call and success counts as well
            filtered_call_counts = {k: v for k, v in self.call_counts.items() if k in function_names}
            filtered_success_counts = {k: v for k, v in self.success_counts.items() if k in function_names}

        else:
            # No filter, use all events and counts
            filtered_events = self.events
            filtered_call_counts = self.call_counts
            filtered_success_counts = self.success_counts
            function_names = set(filtered_call_counts.keys())  # Get names from filtered counts

        # Calculate overall stats
        total_calls = sum(filtered_call_counts.values())
        total_success = sum(filtered_success_counts.values())
        total_time = sum(e.get("Duration (s)", 0.0) for e in filtered_events)  # Use .get with default 0.0
        total_overhead = sum(self.decorator_overhead.get(fn, 0.0) for fn in function_names)  # Sum filtered overhead

        # Calculate overall rates and averages - handle division by zero
        success_rate = (total_success / total_calls * 100) if total_calls else 0
        avg_duration = (total_time / len(filtered_events)) if filtered_events else 0

        # Get slowest and fastest functions - requires filtering events first
        slowest_event = None
        fastest_event = None
        if filtered_events:
            try:
                # Ensure keys exist and values are numeric before finding min/max
                numeric_events = [e for e in filtered_events if isinstance(e.get("Duration (s)"), (int, float))]
                if numeric_events:
                    slowest_event = max(numeric_events, key=lambda e: e["Duration (s)"])
                    fastest_event = min(numeric_events, key=lambda e: e["Duration (s)"])
            except ValueError:  # Handle case where filtered_events might become empty unexpectedly or has non-numeric durations
                pass  # Slowest/fastest remain None

        # Count by duration category - requires filtering events first
        duration_counts = {"fast": 0, "medium": 0, "slow": 0}
        if filtered_events:
            try:
                duration_counts = {
                    "fast": sum(
                        1 for e in filtered_events if isinstance(e.get("Duration (s)"), (int, float)) and e["Duration (s)"] <= self.short_max
                    ),
                    "medium": sum(
                        1
                        for e in filtered_events
                        if isinstance(e.get("Duration (s)"), (int, float)) and self.short_max < e["Duration (s)"] <= self.medium_max
                    ),
                    "slow": sum(
                        1 for e in filtered_events if isinstance(e.get("Duration (s)"), (int, float)) and e["Duration (s)"] > self.medium_max
                    ),
                }
            except Exception as e:
                # Log error if duration counting fails
                if self.error_logger:
                    self.error_logger.error(f"Error counting duration categories: {e}")
                else:
                    print(f"ERROR: Error counting duration categories: {e}", file=sys.stderr)

        # Calculate per-function stats
        function_stats = {}
        for fn in function_names:
            calls = filtered_call_counts.get(fn, 0)
            success = filtered_success_counts.get(fn, 0)
            func_events = [e for e in filtered_events if e.get("Function") == fn]
            func_time = sum(e.get("Duration (s)", 0.0) for e in func_events)
            func_overhead = self.decorator_overhead.get(fn, 0.0)

            func_success_rate = (success / calls * 100) if calls else 0
            func_avg_duration = (func_time / len(func_events)) if func_events else 0

            function_stats[fn] = {
                "total_calls": calls,
                "total_success": success,
                "success_rate": func_success_rate,
                "total_time": func_time,
                "avg_duration": func_avg_duration,
                "total_overhead": func_overhead,
                "event_count": len(func_events),
            }

        # Build final stats dictionary
        stats = {
            "total_calls": total_calls,
            "total_success": total_success,
            "success_rate": success_rate,
            "total_time": total_time,
            "avg_duration": avg_duration,
            "slowest_event": slowest_event,  # Renamed for clarity
            "fastest_event": fastest_event,  # Renamed for clarity
            "duration_counts": duration_counts,
            "function_count": len(function_names),
            "event_count": len(filtered_events),
            "total_decorator_overhead": total_overhead,  # Added total overhead
            "functions": function_stats,  # Added per-function stats
        }

        return stats

    def clear_old_events(self, days: int = 7) -> int:
        """
        Clear events older than specified number of days.
        """
        if not self.events:
            return 0

        # Use datetime.now() for current time comparison
        cutoff_date = datetime.now() - timedelta(days=days)

        # Handle compact time format - remove by count instead of date
        if self.compact_time:
            remove_count = min(len(self.events) // 2, 1000)  # Remove up to half or 1000 events
            if remove_count > 0:
                self.events = self.events[remove_count:]
                if self.console_logger:
                    self.console_logger.debug(f"📊 Pruned {remove_count} oldest events (compact time)")
            return remove_count

        # Regular date-based cleanup when we have full timestamps
        # Ensure 'Start Time' is a string before trying to parse
        # Filter for events with valid string timestamps within the cutoff
        cleaned_events = []
        removed_count = 0
        for e in self.events:
            start_time_str = e.get("Start Time")
            if isinstance(start_time_str, str):
                try:
                    event_date = datetime.strptime(start_time_str, self.time_format)
                    if event_date >= cutoff_date:
                        cleaned_events.append(e)
                    else:
                        removed_count += 1
                except ValueError:
                    # Keep events with invalid date format, log a warning
                    if self.error_logger:
                        self.error_logger.warning(f"Skipping date-based pruning for event with invalid timestamp format: {start_time_str}")
                    else:
                        print(f"WARNING: Skipping date-based pruning for event with invalid timestamp format: {start_time_str}", file=sys.stderr)
                    cleaned_events.append(e)  # Keep event on parsing error
            else:
                # Keep events where 'Start Time' is not a string, log a warning
                if self.error_logger:
                    self.error_logger.warning(f"Skipping date-based pruning for event with non-string timestamp: {start_time_str}")
                else:
                    print(f"WARNING: Skipping date-based pruning for event with non-string timestamp: {start_time_str}", file=sys.stderr)
                cleaned_events.append(e)  # Keep event if timestamp is not a string

        self.events = cleaned_events
        if self.console_logger and removed_count > 0:
            self.console_logger.debug(f"📊 Removed {removed_count} events older than {days} days")

        return removed_count

    def log_summary(self) -> None:
        """
        Log a summary of current analytics data.
        """
        stats = self.get_stats()

        if self.console_logger:
            self.console_logger.info(
                f"📊 Analytics Summary: {stats['total_calls']} calls, " f"{stats['success_rate']:.1f}% success, " f"avg {stats['avg_duration']:.3f}s"
            )

        # Log performance categories
        if stats["event_count"] > 0 and self.console_logger:
            dc = stats["duration_counts"]
            total = sum(dc.values())
            # Avoid division by zero if total is 0
            if total > 0:
                self.console_logger.info(
                    f"📊 Performance: " f"⚡ {dc['fast']/total*100:.0f}% | " f"⏱️ {dc['medium']/total*100:.0f}% | " f"🐢 {dc['slow']/total*100:.0f}%"
                )
            else:
                self.console_logger.info("📊 Performance: No events recorded.")
        elif self.console_logger:
            self.console_logger.info("📊 Performance: No events recorded.")

    def generate_reports(self, force_mode: bool = False) -> None:
        """
        Generate HTML reports from collected analytics data.
        """
        # Check if the save_html_report function is actually available
        if 'save_html_report' not in globals() or not callable(save_html_report):
            if self.console_logger:
                self.console_logger.warning("📊 HTML report generation skipped: save_html_report function not available.")
            return

        try:
            event_count = len(self.events)
            function_count = len(self.call_counts)

            # Generate summary log
            if event_count > 0:
                self.log_summary()

            # Only proceed if we have data
            if event_count == 0 and function_count == 0:
                if self.console_logger:
                    self.console_logger.warning("📊 No analytics data to generate report")
                return

            if self.console_logger:
                self.console_logger.info(f"📊 Generating HTML report ({event_count} events, {function_count} functions)")

            # Generate HTML report using the utility function
            # Pass self.config and self.loggers to the utility function
            save_html_report(
                self.events,
                self.call_counts,
                self.success_counts,
                self.decorator_overhead,
                self.config,
                self.console_logger,
                self.error_logger,
                group_successful_short_calls=True,  # Assuming this option is intended
                force_mode=force_mode,
            )

            # Log which report was generated
            report_type = "full" if force_mode else "incremental"
            if self.analytics_logger:
                self.analytics_logger.info(f"📊 HTML {report_type} report generated successfully")

            # Suggest garbage collection after large report generation
            if event_count > 5000:
                gc.collect()

        except (KeyboardInterrupt, SystemExit) as e:
            # Log interruption using error logger
            if self.error_logger:
                self.error_logger.error(f"📊 HTML report generation interrupted: {e}", exc_info=True)
            if self.analytics_logger:
                self.analytics_logger.error(f"📊 HTML report generation interrupted: {e}")
            # Re-raise to ensure program exits
            raise
        except Exception as e:
            # Log other errors during report generation
            if self.error_logger:
                self.error_logger.error(f"📊 Failed to generate HTML report: {e}", exc_info=True)
            if self.analytics_logger:
                self.analytics_logger.error(f"📊 Failed to generate HTML report: {e}")
