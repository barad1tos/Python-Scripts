#!/usr/bin/env python3
"""
Analytics Module

Provides a lightweight performance and error tracking system for Python functions.
Uses decorators to automatically measure execution time and success rates.

Usage:
    1. Create an instance of the Analytics class
    2. Decorate functions with @analytics.track("Event Type")
    3. Access reports via analytics.generate_reports() or analytics.get_stats()

Example:
    ```python
    from utils.analytics import Analytics
    
    analytics = Analytics(config, console_logger, error_logger, analytics_logger)
    
    @analytics.track("API Call")
    async def fetch_data(url):
        # Your code here
        return response
    
    # Later, generate a report
    analytics.generate_reports()
    
    # Or get a summary
    stats = analytics.get_stats()
    print(f"Success rate: {stats['success_rate']:.1f}%")
    ```
"""

import asyncio
import time

from datetime import datetime, timedelta
from functools import wraps
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

import gc

class Analytics:
    """
    Tracks function performance, success rates, and execution patterns.
    
    Provides decorators for tracking sync/async functions, aggregated statistics,
    and HTML report generation. Includes memory management features to prevent
    unbounded growth in long-running applications.
    
    Attributes:
        instance_id (int): Unique identifier for this analytics instance
        events (List[Dict]): List of tracked function call events
        call_counts (Dict[str, int]): Count of calls per function
        success_counts (Dict[str, int]): Count of successful calls per function
        decorator_overhead (Dict[str, float]): Time overhead added by decorators
        max_events (int): Maximum number of events to store in memory
    """
    # Class-level counter for instance IDs
    _instances = 0
    
    def __init__(
        self, 
        config: Dict[str, Any], 
        console_logger, 
        error_logger, 
        analytics_logger,
        max_events: int = None
    ):
        """
        Initialize the Analytics tracker.

        Args:
            config: Configuration dictionary
            console_logger: Logger for console output
            error_logger: Logger for error messages
            analytics_logger: Logger for analytics data
            max_events: Maximum events to keep in memory (defaults to config value or 10000)
        """
        # Create unique instance identifier
        Analytics._instances += 1
        self.instance_id = Analytics._instances

        # Data storage
        self.events: List[Dict[str, Any]] = []
        self.call_counts: Dict[str, int] = {}
        self.success_counts: Dict[str, int] = {}
        self.decorator_overhead: Dict[str, float] = {}
        
        # Configuration and loggers
        self.config = config
        self.console_logger = console_logger
        self.error_logger = error_logger
        self.analytics_logger = analytics_logger

        # Memory management settings
        self.max_events = max_events or config.get("analytics", {}).get("max_events", 10000)
        
        # Load thresholds from config
        thresholds = config.get("analytics", {}).get("duration_thresholds", {})
        self.short_max = thresholds.get("short_max", 2)
        self.medium_max = thresholds.get("medium_max", 5)
        self.long_max = thresholds.get("long_max", 10)
        
        # Time format settings
        self.time_format = config.get("analytics", {}).get("time_format", "%Y-%m-%d %H:%M:%S")
        self.compact_time = config.get("analytics", {}).get("compact_time", False)
        
        # Log initialization
        self.console_logger.debug(f"📊 Analytics #{self.instance_id} initialized")
    
    def track(self, event_type: str) -> Callable:
        """
        Decorator that tracks function performance and success.
        
        This is the preferred public API (instead of the older 'decorator' method).
        
        Args:
            event_type: Category for the tracked function (e.g., "API Call", "Database Query")
            
        Returns:
            Decorator function that wraps the original function
            
        Example:
            ```
            @analytics.track("File Operation")
            def save_document(filename, content):
                # function code
            ```
        """
        return self.decorator(event_type)
        
    def decorator(self, event_type: str) -> Callable:
        """
        Legacy name for the track method. Provides same functionality.
        
        Args:
            event_type: Category for the tracked function
            
        Returns:
            Decorator function
        """
        def decorator_function(func: Callable) -> Callable:
            # Choose appropriate wrapper based on function type
            if asyncio.iscoroutinefunction(func):
                return self._create_async_wrapper(func, event_type)
            else:
                return self._create_sync_wrapper(func, event_type)
        return decorator_function
    
    def _create_sync_wrapper(self, func: Callable, event_type: str) -> Callable:
        """
        Creates a synchronous wrapper for tracking function performance.
        
        Args:
            func: The function to wrap
            event_type: Event category
            
        Returns:
            Wrapped synchronous function with timing and tracking
        """
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
                self.error_logger.error(f"❌ {func_name}: {str(e)}")
                raise
            finally:
                function_end = time.time()
                decorator_end = time.time()
                duration = function_end - function_start
                overhead = decorator_end - decorator_start - duration
                self._record_function_call(func_name, event_type, function_start, function_end, duration, success, overhead)
        return sync_wrapper
    
    def _create_async_wrapper(self, func: Callable, event_type: str) -> Callable:
        """
        Creates an asynchronous wrapper for tracking function performance.
        
        Args:
            func: The async function to wrap
            event_type: Event category
            
        Returns:
            Wrapped asynchronous function with timing and tracking
        """
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
                self.error_logger.error(f"❌ {func_name}: {str(e)}")
                raise
            finally:
                function_end = time.time()
                decorator_end = time.time()
                duration = function_end - function_start
                overhead = decorator_end - decorator_start - duration
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
        overhead: float
    ) -> None:
        """
        Records performance data for a function call.
        
        Args:
            func_name: Name of the function
            event_type: Type of operation
            start_time: Unix timestamp of start time
            end_time: Unix timestamp of end time
            duration: Execution duration in seconds
            success: Whether the function completed successfully
            overhead: Time added by the decorator itself
        """
        # Manage memory by removing oldest events if needed
        if self.max_events > 0 and len(self.events) >= self.max_events:
            # Remove oldest 10% of events when limit is reached
            remove_count = max(1, int(self.max_events * 0.1))
            self.events = self.events[remove_count:]
            if remove_count > 100:  # Only log if significant pruning happens
                self.console_logger.debug(f"📊 Pruned {remove_count} oldest events")
        
        # Format timestamp based on configuration
        if self.compact_time:
            start_str = datetime.fromtimestamp(start_time).strftime("%H:%M:%S")
            end_str = datetime.fromtimestamp(end_time).strftime("%H:%M:%S")
        else:
            start_str = datetime.fromtimestamp(start_time).strftime(self.time_format)
            end_str = datetime.fromtimestamp(end_time).strftime(self.time_format)
            
        # Add to events list
        self.events.append({
            "Function": func_name,
            "Event Type": event_type,
            "Start Time": start_str,
            "End Time": end_str,
            "Duration (s)": round(duration, 4),
            "Success": success
        })

        # Update counters
        self.call_counts[func_name] = self.call_counts.get(func_name, 0) + 1
        if success:
            self.success_counts[func_name] = self.success_counts.get(func_name, 0) + 1
        
        # Track decorator overhead
        self.decorator_overhead[func_name] = self.decorator_overhead.get(func_name, 0.0) + overhead
        
        # Log the outcome with appropriate duration category and symbol
        status = "✅" if success else "❌"
        duration_symbol = self._get_duration_symbol(duration)
        
        log_message = f"{status} {duration_symbol} {func_name} ({event_type}) took {duration:.3f}s"
        
        if success:
            log_level = "info" if duration > self.long_max else "debug"
            getattr(self.analytics_logger, log_level)(log_message)
            getattr(self.console_logger, log_level)(log_message)
        else:
            self.analytics_logger.warning(log_message)
            self.console_logger.warning(log_message)
    
    def _get_duration_symbol(self, duration: float) -> str:
        """
        Returns a symbol representing the duration category.
        
        Args:
            duration: Time in seconds
            
        Returns:
            Emoji representing speed category (⚡/⏱️/🐢)
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
        
        Args:
            other: Another Analytics instance to merge from
        """
        if other is self:
            return
            
        self.console_logger.debug(f"📊 Merging data from Analytics #{other.instance_id}")
        
        # Merge events with memory limit check
        remaining_capacity = self.max_events - len(self.events) if self.max_events > 0 else float('inf')
        events_to_add = other.events[:remaining_capacity] if remaining_capacity < len(other.events) else other.events
        self.events.extend(events_to_add)
        
        # Merge counters
        for fn, count in other.call_counts.items():
            self.call_counts[fn] = self.call_counts.get(fn, 0) + count
        
        for fn, count in other.success_counts.items():
            self.success_counts[fn] = self.success_counts.get(fn, 0) + count
            
        for fn, overhead in other.decorator_overhead.items():
            self.decorator_overhead[fn] = self.decorator_overhead.get(fn, 0) + overhead
        
        # Clean other instance data
        other.events.clear()
        other.call_counts.clear()
        other.success_counts.clear()
        other.decorator_overhead.clear()
    
    def get_stats(self, function_filter: Union[str, List[str]] = None) -> Dict[str, Any]:
        """
        Get summary statistics for tracked functions.
        
        Args:
            function_filter: Optional function name(s) to filter stats for
            
        Returns:
            Dictionary with aggregated statistics
            
        Example:
            ```
            stats = analytics.get_stats()
            print(f"Average duration: {stats['avg_duration']:.2f}s")
            print(f"Success rate: {stats['success_rate']:.1f}%")
            ```
        """
        # Filter events if needed
        if function_filter:
            if isinstance(function_filter, str):
                function_names = {function_filter}
            else:
                function_names = set(function_filter)
                
            filtered_events = [e for e in self.events if e["Function"] in function_names]
        else:
            filtered_events = self.events
            function_names = set(self.call_counts.keys())
        
        # Count totals
        total_calls = sum(self.call_counts.get(fn, 0) for fn in function_names)
        total_success = sum(self.success_counts.get(fn, 0) for fn in function_names)
        total_time = sum(e["Duration (s)"] for e in filtered_events)
        
        # Calculate rates
        success_rate = (total_success / total_calls * 100) if total_calls else 0
        avg_duration = (total_time / len(filtered_events)) if filtered_events else 0
        
        # Get slowest and fastest functions
        if filtered_events:
            slowest = max(filtered_events, key=lambda e: e["Duration (s)"])
            fastest = min(filtered_events, key=lambda e: e["Duration (s)"])
        else:
            slowest = fastest = None
        
        # Count by duration category
        duration_counts = {
            "fast": sum(1 for e in filtered_events if e["Duration (s)"] <= self.short_max),
            "medium": sum(1 for e in filtered_events if self.short_max < e["Duration (s)"] <= self.medium_max),
            "slow": sum(1 for e in filtered_events if e["Duration (s)"] > self.medium_max)
        }
        
        # Build stats dictionary
        stats = {
            "total_calls": total_calls,
            "total_success": total_success,
            "success_rate": success_rate,
            "total_time": total_time,
            "avg_duration": avg_duration,
            "slowest": slowest,
            "fastest": fastest,
            "duration_counts": duration_counts,
            "function_count": len(function_names),
            "event_count": len(filtered_events)
        }
        
        return stats
    
    def clear_old_events(self, days: int = 7) -> int:
        """
        Clear events older than specified number of days.
        
        Args:
            days: Events older than this many days will be removed
            
        Returns:
            Number of events removed
        """
        if not self.events:
            return 0
            
        cutoff_date = datetime.now() - timedelta(days=days)
        
        # Handle compact time format
        if self.compact_time:
            # When using compact time, we don't track dates, so remove by count instead
            remove_count = min(len(self.events) // 2, 1000)  # Remove up to half or 1000 events
            self.events = self.events[remove_count:]
            return remove_count
            
        # Regular date-based cleanup when we have full timestamps
        original_count = len(self.events)
        self.events = [
            e for e in self.events 
            if datetime.strptime(e["Start Time"], self.time_format) >= cutoff_date
        ]
        
        return original_count - len(self.events)
    
    def log_summary(self) -> None:
        """
        Log a summary of current analytics data.
        """
        stats = self.get_stats()
        
        self.console_logger.info(
            f"📊 Analytics Summary: {stats['total_calls']} calls, "
            f"{stats['success_rate']:.1f}% success, "
            f"avg {stats['avg_duration']:.3f}s"
        )
        
        # Log performance categories
        if stats["event_count"] > 0:
            dc = stats["duration_counts"]
            total = sum(dc.values())
            self.console_logger.info(
                f"📊 Performance: "
                f"⚡ {dc['fast']/total*100:.0f}% | "
                f"⏱️ {dc['medium']/total*100:.0f}% | "
                f"🐢 {dc['slow']/total*100:.0f}%"
            )
    
    def generate_reports(self) -> None:
        """
        Generate HTML reports from collected analytics data.
        """
        try:
            event_count = len(self.events)
            function_count = len(self.call_counts)
            
            # Generate summary log
            if event_count > 0:
                self.log_summary()
            
            # Only proceed if we have data
            if event_count == 0 and function_count == 0:
                self.console_logger.warning("📊 No analytics data to generate report")
                return
                
            self.console_logger.info(f"📊 Generating HTML report ({event_count} events, {function_count} functions)")
                
            # Import here to avoid circular imports
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
            
            self.analytics_logger.info("📊 HTML report generated successfully")
            
            # Suggest garbage collection after large report generation
            if event_count > 5000:
                gc.collect()
                
        except Exception as e:
            self.error_logger.error(f"📊 Failed to generate HTML report: {e}", exc_info=True)
            self.analytics_logger.error(f"📊 Failed to generate HTML report: {e}")