"""
Data validation and quality assurance module for supply chain data
"""

import re
import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass
import json
import hashlib

logger = logging.getLogger(__name__)

@dataclass
class ValidationResult:
    """Result of data validation"""
    is_valid: bool
    quality_score: float
    errors: List[str]
    warnings: List[str]
    metadata: Dict[str, Any]

class DataQualityValidator:
    """Comprehensive data quality validation"""
    
    def __init__(self):
        self.validation_rules = self._load_validation_rules()
        self.quality_thresholds = {
            "minimum_title_length": 10,
            "minimum_description_length": 20,
            "maximum_title_length": 200,
            "maximum_description_length": 2000,
            "severity_range": (0.0, 1.0),
            "minimum_quality_score": 0.3
        }
    
    def _load_validation_rules(self) -> Dict[str, Any]:
        """Load validation rules and patterns"""
        return {
            "required_fields": ["title", "description", "severity"],
            "optional_fields": ["location", "impact_sectors", "source", "url"],
            "severity_keywords": {
                "high": ["crisis", "emergency", "critical", "severe", "major", "catastrophic"],
                "medium": ["disruption", "delay", "shortage", "impact", "concern", "warning"],
                "low": ["minor", "slight", "limited", "temporary", "brief"]
            },
            "location_patterns": [
                r'\b[A-Z][a-z]+ [A-Z][a-z]+\b',  # City State
                r'\b[A-Z][a-z]+, [A-Z]{2}\b',    # City, ST
                r'\b[A-Z][a-z]+ Canal\b',        # Canal names
                r'\b[A-Z][a-z]+ Port\b'          # Port names
            ],
            "sector_keywords": {
                "automotive": ["car", "auto", "vehicle", "toyota", "ford", "gm"],
                "electronics": ["chip", "semiconductor", "electronics", "tech"],
                "energy": ["oil", "gas", "energy", "power", "electricity"],
                "transportation": ["shipping", "logistics", "freight", "cargo", "port"],
                "manufacturing": ["factory", "plant", "production", "manufacturing"],
                "agriculture": ["food", "farming", "crop", "grain", "livestock"]
            }
        }
    
    def validate_event(self, event: Dict[str, Any]) -> ValidationResult:
        """Comprehensive validation of a single event"""
        errors = []
        warnings = []
        quality_score = 0.0
        metadata = {}
        
        # Required fields validation
        for field in self.validation_rules["required_fields"]:
            if not event.get(field):
                errors.append(f"Missing required field: {field}")
            elif not str(event[field]).strip():
                errors.append(f"Empty required field: {field}")
        
        # Title validation
        title = event.get("title", "")
        if title:
            title_quality = self._validate_title(title)
            quality_score += title_quality["score"]
            errors.extend(title_quality["errors"])
            warnings.extend(title_quality["warnings"])
            metadata["title_analysis"] = title_quality["metadata"]
        
        # Description validation
        description = event.get("description", "")
        if description:
            desc_quality = self._validate_description(description)
            quality_score += desc_quality["score"]
            errors.extend(desc_quality["errors"])
            warnings.extend(desc_quality["warnings"])
            metadata["description_analysis"] = desc_quality["metadata"]
        
        # Severity validation
        severity = event.get("severity")
        if severity is not None:
            severity_quality = self._validate_severity(severity, title + " " + description)
            quality_score += severity_quality["score"]
            errors.extend(severity_quality["errors"])
            warnings.extend(severity_quality["warnings"])
            metadata["severity_analysis"] = severity_quality["metadata"]
        
        # Location validation
        location = event.get("location", "")
        if location:
            location_quality = self._validate_location(location)
            quality_score += location_quality["score"]
            warnings.extend(location_quality["warnings"])
            metadata["location_analysis"] = location_quality["metadata"]
        
        # Sectors validation
        sectors = event.get("impact_sectors", [])
        if sectors:
            sectors_quality = self._validate_sectors(sectors, title + " " + description)
            quality_score += sectors_quality["score"]
            warnings.extend(sectors_quality["warnings"])
            metadata["sectors_analysis"] = sectors_quality["metadata"]
        
        # URL validation
        url = event.get("url", "")
        if url:
            url_quality = self._validate_url(url)
            warnings.extend(url_quality["warnings"])
            metadata["url_analysis"] = url_quality["metadata"]
        
        # Overall quality assessment
        is_valid = len(errors) == 0 and quality_score >= self.quality_thresholds["minimum_quality_score"]
        
        return ValidationResult(
            is_valid=is_valid,
            quality_score=min(1.0, quality_score),
            errors=errors,
            warnings=warnings,
            metadata=metadata
        )
    
    def _validate_title(self, title: str) -> Dict[str, Any]:
        """Validate event title"""
        errors = []
        warnings = []
        score = 0.0
        metadata = {}
        
        title_len = len(title.strip())
        metadata["length"] = title_len
        
        # Length validation
        if title_len < self.quality_thresholds["minimum_title_length"]:
            errors.append(f"Title too short: {title_len} characters (minimum: {self.quality_thresholds['minimum_title_length']})")
        elif title_len > self.quality_thresholds["maximum_title_length"]:
            warnings.append(f"Title very long: {title_len} characters (maximum recommended: {self.quality_thresholds['maximum_title_length']})")
            score += 0.15  # Still give some score for having content
        else:
            score += 0.2  # Full score for appropriate length
        
        # Content quality
        if title.strip():
            # Check for meaningful content (not just special characters)
            meaningful_chars = len(re.sub(r'[^\w\s]', '', title))
            if meaningful_chars / title_len > 0.7:
                score += 0.1
            else:
                warnings.append("Title contains many special characters")
            
            # Check for proper capitalization
            if title[0].isupper():
                score += 0.05
            
            # Check for supply chain relevance
            supply_chain_terms = ["supply", "chain", "logistics", "shipping", "port", "manufacturing", "disruption"]
            if any(term in title.lower() for term in supply_chain_terms):
                score += 0.1
                metadata["supply_chain_relevant"] = True
        
        return {
            "score": score,
            "errors": errors,
            "warnings": warnings,
            "metadata": metadata
        }
    
    def _validate_description(self, description: str) -> Dict[str, Any]:
        """Validate event description"""
        errors = []
        warnings = []
        score = 0.0
        metadata = {}
        
        desc_len = len(description.strip())
        metadata["length"] = desc_len
        
        # Length validation
        if desc_len < self.quality_thresholds["minimum_description_length"]:
            errors.append(f"Description too short: {desc_len} characters (minimum: {self.quality_thresholds['minimum_description_length']})")
        elif desc_len > self.quality_thresholds["maximum_description_length"]:
            warnings.append(f"Description very long: {desc_len} characters (maximum recommended: {self.quality_thresholds['maximum_description_length']})")
            score += 0.25  # Still give score for having content
        else:
            score += 0.3  # Full score for appropriate length
        
        # Content quality
        if description.strip():
            # Sentence structure
            sentences = description.split('.')
            metadata["sentence_count"] = len([s for s in sentences if s.strip()])
            
            if len(sentences) > 1:
                score += 0.1  # Bonus for multiple sentences
            
            # Information density
            words = description.split()
            metadata["word_count"] = len(words)
            
            if len(words) > 10:
                score += 0.1  # Bonus for detailed description
        
        return {
            "score": score,
            "errors": errors,
            "warnings": warnings,
            "metadata": metadata
        }
    
    def _validate_severity(self, severity: Any, content: str) -> Dict[str, Any]:
        """Validate severity score"""
        errors = []
        warnings = []
        score = 0.0
        metadata = {}
        
        # Type and range validation
        try:
            severity_float = float(severity)
            metadata["severity_value"] = severity_float
            
            if not (self.quality_thresholds["severity_range"][0] <= severity_float <= self.quality_thresholds["severity_range"][1]):
                errors.append(f"Severity out of range: {severity_float} (must be between 0.0 and 1.0)")
            else:
                score += 0.2  # Base score for valid range
                
                # Content consistency check
                content_lower = content.lower()
                
                # High severity keywords
                high_severity_found = any(keyword in content_lower for keyword in self.validation_rules["severity_keywords"]["high"])
                medium_severity_found = any(keyword in content_lower for keyword in self.validation_rules["severity_keywords"]["medium"])
                low_severity_found = any(keyword in content_lower for keyword in self.validation_rules["severity_keywords"]["low"])
                
                # Consistency scoring
                if severity_float >= 0.7 and high_severity_found:
                    score += 0.1  # Consistent high severity
                    metadata["consistency"] = "high_consistent"
                elif severity_float >= 0.4 and medium_severity_found:
                    score += 0.1  # Consistent medium severity
                    metadata["consistency"] = "medium_consistent"
                elif severity_float < 0.4 and low_severity_found:
                    score += 0.1  # Consistent low severity
                    metadata["consistency"] = "low_consistent"
                elif severity_float >= 0.7 and not high_severity_found:
                    warnings.append("High severity score but no high-severity keywords found in content")
                    metadata["consistency"] = "high_inconsistent"
                elif severity_float < 0.4 and high_severity_found:
                    warnings.append("Low severity score but high-severity keywords found in content")
                    metadata["consistency"] = "low_inconsistent"
                else:
                    metadata["consistency"] = "neutral"
        
        except (ValueError, TypeError):
            errors.append(f"Invalid severity type: {type(severity)} (must be numeric)")
        
        return {
            "score": score,
            "errors": errors,
            "warnings": warnings,
            "metadata": metadata
        }
    
    def _validate_location(self, location: str) -> Dict[str, Any]:
        """Validate location information"""
        warnings = []
        score = 0.0
        metadata = {}
        
        location_clean = location.strip()
        metadata["original_location"] = location_clean
        
        if location_clean:
            score += 0.1  # Base score for having location
            
            # Pattern matching for structured locations
            for pattern in self.validation_rules["location_patterns"]:
                if re.search(pattern, location_clean):
                    score += 0.1
                    metadata["structured_format"] = True
                    break
            else:
                metadata["structured_format"] = False
            
            # Known location check (simplified)
            known_locations = ["china", "usa", "germany", "singapore", "los angeles", "shanghai", "rotterdam"]
            if any(known in location_clean.lower() for known in known_locations):
                score += 0.05
                metadata["known_location"] = True
            else:
                warnings.append(f"Unknown location: {location_clean}")
                metadata["known_location"] = False
        
        return {
            "score": score,
            "errors": [],
            "warnings": warnings,
            "metadata": metadata
        }
    
    def _validate_sectors(self, sectors: List[str], content: str) -> Dict[str, Any]:
        """Validate impact sectors"""
        warnings = []
        score = 0.0
        metadata = {}
        
        if not isinstance(sectors, list):
            warnings.append("Sectors should be a list")
            return {"score": 0.0, "errors": [], "warnings": warnings, "metadata": metadata}
        
        metadata["sector_count"] = len(sectors)
        
        if sectors:
            score += 0.1  # Base score for having sectors
            
            # Content consistency check
            content_lower = content.lower()
            consistent_sectors = []
            
            for sector in sectors:
                sector_lower = sector.lower().strip()
                
                # Check if sector keywords appear in content
                sector_keywords = self.validation_rules["sector_keywords"].get(sector_lower, [])
                if sector_keywords and any(keyword in content_lower for keyword in sector_keywords):
                    consistent_sectors.append(sector)
                    score += 0.05  # Bonus for content-consistent sectors
            
            metadata["consistent_sectors"] = consistent_sectors
            metadata["consistency_ratio"] = len(consistent_sectors) / len(sectors) if sectors else 0
            
            if len(consistent_sectors) < len(sectors) / 2:
                warnings.append("Many sectors not supported by content keywords")
        
        return {
            "score": score,
            "errors": [],
            "warnings": warnings,
            "metadata": metadata
        }
    
    def _validate_url(self, url: str) -> Dict[str, Any]:
        """Validate URL format"""
        warnings = []
        metadata = {}
        
        url_pattern = re.compile(
            r'^https?://'  # http:// or https://
            r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|'  # domain...
            r'localhost|'  # localhost...
            r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
            r'(?::\d+)?'  # optional port
            r'(?:/?|[/?]\S+)$', re.IGNORECASE)
        
        if not url_pattern.match(url):
            warnings.append(f"Invalid URL format: {url}")
            metadata["valid_format"] = False
        else:
            metadata["valid_format"] = True
            
            # Check for HTTPS
            if url.startswith('https://'):
                metadata["secure"] = True
            else:
                warnings.append("URL uses HTTP instead of HTTPS")
                metadata["secure"] = False
        
        return {
            "score": 0.0,  # URL doesn't contribute to quality score
            "errors": [],
            "warnings": warnings,
            "metadata": metadata
        }

class DuplicateDetector:
    """Advanced duplicate detection system"""
    
    def __init__(self, similarity_threshold: float = 0.85):
        self.similarity_threshold = similarity_threshold
        self.processed_events = {}  # Hash -> event metadata
        self.content_signatures = {}  # Simplified content -> hash
    
    def is_duplicate(self, event: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """Check if event is a duplicate"""
        # Generate multiple hashes for different levels of similarity
        exact_hash = self._generate_exact_hash(event)
        content_hash = self._generate_content_hash(event)
        fuzzy_hash = self._generate_fuzzy_hash(event)
        
        # Check exact duplicates first
        if exact_hash in self.processed_events:
            return True, "exact_duplicate"
        
        # Check content duplicates
        if content_hash in self.processed_events:
            return True, "content_duplicate"
        
        # Check fuzzy duplicates
        if fuzzy_hash in self.processed_events:
            return True, "fuzzy_duplicate"
        
        # Store hashes for future comparison
        event_metadata = {
            "timestamp": datetime.utcnow().isoformat(),
            "title": event.get("title", "")[:50],  # Store first 50 chars
            "source": event.get("source", "unknown")
        }
        
        self.processed_events[exact_hash] = event_metadata
        self.processed_events[content_hash] = event_metadata
        self.processed_events[fuzzy_hash] = event_metadata
        
        return False, None
    
    def _generate_exact_hash(self, event: Dict[str, Any]) -> str:
        """Generate hash for exact duplicate detection"""
        content = f"{event.get('title', '')}{event.get('description', '')}{event.get('location', '')}"
        return hashlib.md5(content.encode()).hexdigest()
    
    def _generate_content_hash(self, event: Dict[str, Any]) -> str:
        """Generate hash for content-based duplicate detection"""
        # Normalize content by removing extra spaces and converting to lowercase
        title = re.sub(r'\s+', ' ', event.get('title', '').lower().strip())
        description = re.sub(r'\s+', ' ', event.get('description', '').lower().strip())
        content = f"{title}{description}"
        return hashlib.md5(content.encode()).hexdigest()
    
    def _generate_fuzzy_hash(self, event: Dict[str, Any]) -> str:
        """Generate hash for fuzzy duplicate detection"""
        # Extract key terms and create a simplified signature
        title = event.get('title', '').lower()
        description = event.get('description', '').lower()
        
        # Extract meaningful words (remove common words)
        stop_words = {'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by', 'is', 'are', 'was', 'were'}
        
        all_words = re.findall(r'\b\w+\b', title + ' ' + description)
        meaningful_words = [word for word in all_words if len(word) > 3 and word not in stop_words]
        
        # Sort and join to create signature
        signature = ' '.join(sorted(meaningful_words[:10]))  # Use top 10 meaningful words
        return hashlib.md5(signature.encode()).hexdigest()
    
    def cleanup_old_entries(self, max_age_hours: int = 24):
        """Clean up old entries to prevent memory bloat"""
        cutoff_time = datetime.utcnow() - timedelta(hours=max_age_hours)
        
        to_remove = []
        for hash_key, metadata in self.processed_events.items():
            try:
                event_time = datetime.fromisoformat(metadata["timestamp"])
                if event_time < cutoff_time:
                    to_remove.append(hash_key)
            except (ValueError, KeyError):
                # Remove entries with invalid timestamps
                to_remove.append(hash_key)
        
        for hash_key in to_remove:
            del self.processed_events[hash_key]
        
        logger.info(f"Cleaned up {len(to_remove)} old duplicate detection entries")

class DataNormalizer:
    """Data normalization and standardization"""
    
    def __init__(self):
        self.location_aliases = self._load_location_aliases()
        self.sector_mappings = self._load_sector_mappings()
    
    def _load_location_aliases(self) -> Dict[str, str]:
        """Load location aliases for standardization"""
        return {
            "la": "Los Angeles",
            "nyc": "New York City",
            "sf": "San Francisco",
            "uk": "United Kingdom",
            "usa": "United States",
            "us": "United States",
            "prc": "China",
            "hk": "Hong Kong",
            "sg": "Singapore"
        }
    
    def _load_sector_mappings(self) -> Dict[str, str]:
        """Load sector standardization mappings"""
        return {
            "auto": "automotive",
            "cars": "automotive",
            "vehicles": "automotive",
            "chips": "electronics",
            "semiconductors": "electronics",
            "tech": "electronics",
            "it": "electronics",
            "oil": "energy",
            "gas": "energy",
            "petroleum": "energy",
            "power": "energy",
            "electricity": "energy",
            "food": "agriculture",
            "farming": "agriculture",
            "crops": "agriculture",
            "livestock": "agriculture",
            "shipping": "transportation",
            "logistics": "transportation",
            "freight": "transportation",
            "cargo": "transportation",
            "ports": "transportation",
            "factories": "manufacturing",
            "production": "manufacturing",
            "plants": "manufacturing",
            "industrial": "manufacturing"
        }
    
    def normalize_event(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize event data"""
        normalized_event = event.copy()
        
        # Normalize location
        location = event.get("location", "")
        if location:
            normalized_event["location"] = self._normalize_location(location)
        
        # Normalize sectors
        sectors = event.get("impact_sectors", [])
        if sectors:
            normalized_event["impact_sectors"] = self._normalize_sectors(sectors)
        
        # Normalize text fields
        for field in ["title", "description"]:
            if field in normalized_event:
                normalized_event[field] = self._normalize_text(normalized_event[field])
        
        # Add normalization metadata
        normalized_event["normalization_applied"] = True
        normalized_event["normalized_at"] = datetime.utcnow().isoformat()
        
        return normalized_event
    
    def _normalize_location(self, location: str) -> str:
        """Normalize location string"""
        location_lower = location.lower().strip()
        
        # Check aliases
        if location_lower in self.location_aliases:
            return self.location_aliases[location_lower]
        
        # Basic cleanup
        location_clean = location.strip()
        
        # Capitalize properly
        return ' '.join(word.capitalize() for word in location_clean.split())
    
    def _normalize_sectors(self, sectors: List[str]) -> List[str]:
        """Normalize sector names"""
        normalized = []
        
        for sector in sectors:
            sector_lower = sector.lower().strip()
            
            # Check mappings
            if sector_lower in self.sector_mappings:
                normalized_sector = self.sector_mappings[sector_lower]
                if normalized_sector not in normalized:
                    normalized.append(normalized_sector)
            else:
                # Keep original if no mapping found
                if sector.lower() not in normalized:
                    normalized.append(sector.lower())
        
        return normalized
    
    def _normalize_text(self, text: str) -> str:
        """Normalize text content"""
        if not text:
            return ""
        
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text.strip())
        
        # Fix common encoding issues
        text = text.replace('"', '"').replace('"', '"')
        text = text.replace(''', "'").replace(''', "'")
        
        # Remove control characters
        text = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', text)
        
        return text
