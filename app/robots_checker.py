#!/usr/bin/env python3
"""
robots.txt Compliance Checker

Checks robots.txt before crawling and logs blocked URLs.
"""

import csv
import os
import urllib.robotparser
from datetime import datetime, timezone
from typing import Dict, Optional
from urllib.parse import urlparse


class RobotsChecker:
    """
    Check robots.txt compliance before crawling.
    
    Caches robots.txt parsers per domain to avoid repeated fetches.
    """
    
    def __init__(self, user_agent: str):
        """
        Initialize robots checker.
        
        Args:
            user_agent: User-agent string to use for robots.txt checks
        """
        self.user_agent = user_agent
        self.cache: Dict[str, Optional[urllib.robotparser.RobotFileParser]] = {}
    
    def is_allowed(self, url: str) -> bool:
        """
        Check if URL is allowed by robots.txt.
        
        Args:
            url: URL to check
        
        Returns:
            True if allowed, False if disallowed
        """
        try:
            parsed = urlparse(url)
            domain = parsed.netloc
            
            if not domain:
                return True  # Allow if no domain
            
            # Check cache
            if domain not in self.cache:
                # Fetch and parse robots.txt
                rp = urllib.robotparser.RobotFileParser()
                robots_url = f"{parsed.scheme}://{domain}/robots.txt"
                rp.set_url(robots_url)
                
                try:
                    rp.read()
                    self.cache[domain] = rp
                except Exception:
                    # Allow on fetch failure (robots.txt may not exist)
                    self.cache[domain] = None
            
            # Check if allowed
            if self.cache[domain] is None:
                return True  # Allow if robots.txt not available
            
            return self.cache[domain].can_fetch(self.user_agent, url)
        
        except Exception:
            # Allow on any error
            return True
    
    def log_block(self, authority: str, url: str, reason: str = "disallowed by robots.txt"):
        """
        Log a blocked URL to CSV.
        
        Args:
            authority: Authority code (e.g., MAS, IMDA)
            url: Blocked URL
            reason: Reason for block
        """
        try:
            os.makedirs("data/output/validation/latest", exist_ok=True)
            csv_path = "data/output/validation/latest/robots_blocked.csv"
            
            file_exists = os.path.exists(csv_path)
            
            with open(csv_path, "a", encoding="utf-8", newline="") as f:
                writer = csv.writer(f)
                
                if not file_exists:
                    writer.writerow(["authority", "url", "reason", "timestamp"])
                
                writer.writerow([
                    authority or "",
                    url,
                    reason,
                    datetime.now(timezone.utc).isoformat()
                ])
        
        except Exception:
            pass  # Silently fail on logging errors
    
    def get_stats(self) -> Dict:
        """
        Get statistics about robots.txt checks.
        
        Returns:
            Dict with domains_checked, domains_with_robots
        """
        domains_with_robots = sum(1 for rp in self.cache.values() if rp is not None)
        
        return {
            "domains_checked": len(self.cache),
            "domains_with_robots": domains_with_robots,
            "domains_without_robots": len(self.cache) - domains_with_robots
        }

