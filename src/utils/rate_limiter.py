"""
Rate limiter utility for SEC EDGAR API compliance.
SEC allows maximum 10 requests per second.
"""
import time
import threading
from collections import deque
from typing import Optional
import logging

logger = logging.getLogger(__name__)


class RateLimiter:
    """
    Thread-safe rate limiter for SEC API requests.
    Maintains a sliding window of request timestamps.
    """
    
    def __init__(self, max_requests: int = 8, time_window: int = 1):
        """
        Initialize rate limiter.
        
        Args:
            max_requests: Maximum number of requests allowed
            time_window: Time window in seconds
        """
        self.max_requests = max_requests
        self.time_window = time_window
        self.requests = deque()
        self.lock = threading.Lock()
        
        logger.info(f"Rate limiter initialized: {max_requests} requests per {time_window} second(s)")
    
    def wait_if_needed(self) -> None:
        """
        Wait if necessary to maintain rate limit compliance.
        This method blocks until it's safe to make a request.
        """
        with self.lock:
            current_time = time.time()
            
            # Remove old requests outside the time window
            while self.requests and self.requests[0] <= current_time - self.time_window:
                self.requests.popleft()
            
            # If we're at the limit, wait until the oldest request expires
            if len(self.requests) >= self.max_requests:
                wait_time = self.requests[0] + self.time_window - current_time
                if wait_time > 0:
                    logger.debug(f"Rate limit reached, waiting {wait_time:.2f} seconds")
                    time.sleep(wait_time)
                    # Remove the expired request
                    self.requests.popleft()
            
            # Record this request
            self.requests.append(current_time)
    
    def can_make_request(self) -> bool:
        """
        Check if a request can be made without waiting.
        
        Returns:
            True if a request can be made immediately, False otherwise
        """
        with self.lock:
            current_time = time.time()
            
            # Remove old requests outside the time window
            while self.requests and self.requests[0] <= current_time - self.time_window:
                self.requests.popleft()
            
            return len(self.requests) < self.max_requests
    
    def get_wait_time(self) -> float:
        """
        Get the time to wait before the next request can be made.
        
        Returns:
            Wait time in seconds, 0 if no wait is needed
        """
        with self.lock:
            current_time = time.time()
            
            # Remove old requests outside the time window
            while self.requests and self.requests[0] <= current_time - self.time_window:
                self.requests.popleft()
            
            if len(self.requests) >= self.max_requests:
                return self.requests[0] + self.time_window - current_time
            return 0.0
    
    def reset(self):
        """Reset the rate limiter, clearing all recorded requests"""
        with self.lock:
            self.requests.clear()
            logger.info("Rate limiter reset")
    
    def get_current_usage(self) -> int:
        """
        Get the current number of requests in the time window.
        
        Returns:
            Number of requests in the current time window
        """
        with self.lock:
            current_time = time.time()
            
            # Remove old requests outside the time window
            while self.requests and self.requests[0] <= current_time - self.time_window:
                self.requests.popleft()
            
            return len(self.requests)


class AdaptiveRateLimiter(RateLimiter):
    """
    Adaptive rate limiter that can adjust limits based on server responses.
    Useful for handling 429 (Too Many Requests) responses.
    """
    
    def __init__(self, max_requests: int = 8, time_window: int = 1, 
                 min_requests: int = 1, backoff_factor: float = 0.5):
        """
        Initialize adaptive rate limiter.
        
        Args:
            max_requests: Maximum number of requests allowed
            time_window: Time window in seconds
            min_requests: Minimum number of requests to allow
            backoff_factor: Factor to reduce rate by when backing off
        """
        super().__init__(max_requests, time_window)
        self.original_max_requests = max_requests
        self.min_requests = min_requests
        self.backoff_factor = backoff_factor
        self.consecutive_successes = 0
        self.backoff_active = False
        
        logger.info(f"Adaptive rate limiter initialized: {max_requests} requests per {time_window} second(s)")
    
    def handle_rate_limit_exceeded(self):
        """
        Handle a rate limit exceeded response (429 error).
        Reduces the rate limit temporarily.
        """
        with self.lock:
            old_max = self.max_requests
            self.max_requests = max(
                self.min_requests,
                int(self.max_requests * self.backoff_factor)
            )
            self.consecutive_successes = 0
            self.backoff_active = True
            
            logger.warning(f"Rate limit exceeded, reducing from {old_max} to {self.max_requests} requests per second")
    
    def handle_successful_request(self):
        """
        Handle a successful request.
        Gradually increases rate limit back to normal if it was reduced.
        """
        if not self.backoff_active:
            return
        
        with self.lock:
            self.consecutive_successes += 1
            
            # After 10 consecutive successes, increase rate limit
            if self.consecutive_successes >= 10:
                old_max = self.max_requests
                self.max_requests = min(
                    self.original_max_requests,
                    self.max_requests + 1
                )
                self.consecutive_successes = 0
                
                if self.max_requests >= self.original_max_requests:
                    self.backoff_active = False
                    logger.info("Rate limit restored to normal")
                else:
                    logger.info(f"Rate limit increased from {old_max} to {self.max_requests} requests per second")


# Global rate limiter instance
_rate_limiter = None


def get_rate_limiter(max_requests: int = 8, time_window: int = 1, 
                    adaptive: bool = True) -> RateLimiter:
    """
    Get global rate limiter instance.
    
    Args:
        max_requests: Maximum requests per time window
        time_window: Time window in seconds
        adaptive: Whether to use adaptive rate limiting
    
    Returns:
        RateLimiter instance
    """
    global _rate_limiter
    if _rate_limiter is None:
        if adaptive:
            _rate_limiter = AdaptiveRateLimiter(max_requests, time_window)
        else:
            _rate_limiter = RateLimiter(max_requests, time_window)
    return _rate_limiter 