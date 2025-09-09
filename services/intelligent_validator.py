"""
Intelligent schema inference and dynamic validation system.
Automatically detects data patterns and creates validation rules for any dataset.
"""

import pandas as pd
import numpy as np
import re
from typing import Dict, Any, List, Optional, Union, Tuple
from datetime import datetime
import logging
from dataclasses import dataclass, field
from enum import Enum
import json

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataType(str, Enum):
    """Detected data types"""
    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    DATE = "date"
    DATETIME = "datetime"
    EMAIL = "email"
    PHONE = "phone"
    URL = "url"
    UUID = "uuid"
    CURRENCY = "currency"
    PERCENTAGE = "percentage"
    POSTAL_CODE = "postal_code"
    IP_ADDRESS = "ip_address"
    CREDIT_CARD = "credit_card"
    SOCIAL_SECURITY = "social_security"
    CATEGORICAL = "categorical"
    JSON = "json"
    UNKNOWN = "unknown"


class QualityIssue(str, Enum):
    """Types of data quality issues"""
    MISSING_VALUE = "missing_value"
    INVALID_FORMAT = "invalid_format"
    OUT_OF_RANGE = "out_of_range"
    INCONSISTENT_PATTERN = "inconsistent_pattern"
    DUPLICATE_VALUE = "duplicate_value"
    OUTLIER = "outlier"
    ENCODING_ISSUE = "encoding_issue"
    LENGTH_VIOLATION = "length_violation"


@dataclass
class FieldProfile:
    """Profile of a data field with inferred characteristics"""
    name: str
    detected_type: DataType
    nullable: bool = True
    unique_values: int = 0
    total_values: int = 0
    null_count: int = 0
    sample_values: List[Any] = field(default_factory=list)
    
    # Pattern analysis
    patterns: List[str] = field(default_factory=list)
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    avg_length: Optional[float] = None
    
    # Numeric analysis
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    mean_value: Optional[float] = None
    std_value: Optional[float] = None
    
    # Categorical analysis
    is_categorical: bool = False
    categories: List[Any] = field(default_factory=list)
    category_frequencies: Dict[Any, int] = field(default_factory=dict)
    
    # Quality metrics
    completeness_score: float = 0.0
    consistency_score: float = 0.0
    validity_score: float = 0.0
    overall_quality_score: float = 0.0
    
    # Validation rules
    validation_rules: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ValidationResult:
    """Result of validating a single record"""
    record_id: int
    is_valid: bool = True
    quality_score: float = 1.0
    field_scores: Dict[str, float] = field(default_factory=dict)
    issues: List[Tuple[str, QualityIssue, str]] = field(default_factory=list)  # (field, issue_type, description)
    warnings: List[str] = field(default_factory=list)


class IntelligentSchemaInference:
    """Intelligent schema inference engine"""
    
    def __init__(self):
        self.patterns = self._initialize_patterns()
        self.type_detectors = self._initialize_type_detectors()
    
    def _initialize_patterns(self) -> Dict[DataType, List[str]]:
        """Initialize regex patterns for data type detection"""
        return {
            DataType.EMAIL: [
                r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
            ],
            DataType.PHONE: [
                r'^\+?1?[-.\s]?\(?([0-9]{3})\)?[-.\s]?([0-9]{3})[-.\s]?([0-9]{4})$',
                r'^\+?[1-9]\d{1,14}$'  # E.164 format
            ],
            DataType.URL: [
                r'^https?://(?:[-\w.])+(?:\:[0-9]+)?(?:/(?:[\w/_.])*(?:\?(?:[\w&=%.])*)?(?:\#(?:[\w.])*)?)?$'
            ],
            DataType.UUID: [
                r'^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
            ],
            DataType.CURRENCY: [
                r'^\$?[\d,]+\.?\d*$',
                r'^[\d,]+\.?\d*\s?(USD|EUR|GBP|JPY|CAD|AUD)$'
            ],
            DataType.PERCENTAGE: [
                r'^\d+\.?\d*%$'
            ],
            DataType.POSTAL_CODE: [
                r'^\d{5}(-\d{4})?$',  # US ZIP
                r'^[A-Z]\d[A-Z]\s?\d[A-Z]\d$',  # Canadian
                r'^\d{4,6}$'  # Generic
            ],
            DataType.IP_ADDRESS: [
                r'^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$'
            ],
            DataType.CREDIT_CARD: [
                r'^\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}$'
            ],
            DataType.SOCIAL_SECURITY: [
                r'^\d{3}-?\d{2}-?\d{4}$'
            ]
        }
    
    def _initialize_type_detectors(self) -> Dict[DataType, callable]:
        """Initialize type detection functions"""
        return {
            DataType.INTEGER: self._is_integer,
            DataType.FLOAT: self._is_float,
            DataType.BOOLEAN: self._is_boolean,
            DataType.DATE: self._is_date,
            DataType.DATETIME: self._is_datetime,
            DataType.JSON: self._is_json
        }
    
    def _is_integer(self, value: str) -> bool:
        """Check if value is an integer"""
        try:
            int(value)
            return True
        except (ValueError, TypeError):
            return False
    
    def _is_float(self, value: str) -> bool:
        """Check if value is a float"""
        try:
            float(value)
            return '.' in str(value)
        except (ValueError, TypeError):
            return False
    
    def _is_boolean(self, value: str) -> bool:
        """Check if value is a boolean"""
        if isinstance(value, bool):
            return True
        return str(value).lower() in ['true', 'false', 'yes', 'no', '1', '0', 'y', 'n']
    
    def _is_date(self, value: str) -> bool:
        """Check if value is a date"""
        date_patterns = [
            '%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y', '%Y/%m/%d',
            '%d-%m-%Y', '%m-%d-%Y', '%B %d, %Y', '%d %B %Y'
        ]
        
        for pattern in date_patterns:
            try:
                datetime.strptime(str(value), pattern)
                return True
            except ValueError:
                continue
        return False
    
    def _is_datetime(self, value: str) -> bool:
        """Check if value is a datetime"""
        datetime_patterns = [
            '%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M:%S.%f',
            '%Y-%m-%dT%H:%M:%S', '%Y-%m-%dT%H:%M:%S.%f',
            '%Y-%m-%dT%H:%M:%SZ', '%m/%d/%Y %H:%M:%S'
        ]
        
        for pattern in datetime_patterns:
            try:
                datetime.strptime(str(value), pattern)
                return True
            except ValueError:
                continue
        return False
    
    def _is_json(self, value: str) -> bool:
        """Check if value is JSON"""
        try:
            json.loads(str(value))
            return True
        except (ValueError, TypeError):
            return False
    
    def detect_field_type(self, series: pd.Series) -> DataType:
        """Detect the most likely data type for a field"""
        # Remove null values for analysis
        non_null_series = series.dropna()
        
        if len(non_null_series) == 0:
            return DataType.STRING
        
        # Convert to string for pattern matching
        string_series = non_null_series.astype(str)
        sample_size = min(100, len(string_series))
        sample_values = string_series.sample(sample_size).tolist()
        
        # Track detection scores
        type_scores = {}
        
        # Check pattern-based types
        for data_type, patterns in self.patterns.items():
            matches = 0
            for value in sample_values:
                for pattern in patterns:
                    if re.match(pattern, value, re.IGNORECASE):
                        matches += 1
                        break
            
            if matches > 0:
                type_scores[data_type] = matches / len(sample_values)
        
        # Check function-based types
        for data_type, detector_func in self.type_detectors.items():
            matches = sum(1 for value in sample_values if detector_func(value))
            if matches > 0:
                type_scores[data_type] = matches / len(sample_values)
        
        # Check if categorical
        unique_ratio = len(non_null_series.unique()) / len(non_null_series)
        if unique_ratio < 0.1 and len(non_null_series.unique()) < 50:
            type_scores[DataType.CATEGORICAL] = 1.0
        
        # Return type with highest score
        if type_scores:
            best_type = max(type_scores.items(), key=lambda x: x[1])
            if best_type[1] >= 0.8:  # High confidence threshold
                return best_type[0]
        
        return DataType.STRING  # Default fallback
    
    def profile_field(self, series: pd.Series, field_name: str) -> FieldProfile:
        """Create a comprehensive profile of a data field"""
        profile = FieldProfile(name=field_name, detected_type=DataType.UNKNOWN)
        
        # Basic statistics
        profile.total_values = len(series)
        profile.null_count = series.isnull().sum()
        profile.unique_values = series.nunique()
        profile.nullable = profile.null_count > 0
        
        # Get non-null values
        non_null_series = series.dropna()
        
        if len(non_null_series) == 0:
            profile.detected_type = DataType.STRING
            return profile
        
        # Sample values
        profile.sample_values = non_null_series.sample(min(10, len(non_null_series))).tolist()
        
        # Detect data type
        profile.detected_type = self.detect_field_type(series)
        
        # String length analysis
        if profile.detected_type in [DataType.STRING, DataType.EMAIL, DataType.PHONE, DataType.URL]:
            string_lengths = non_null_series.astype(str).str.len()
            profile.min_length = int(string_lengths.min())
            profile.max_length = int(string_lengths.max())
            profile.avg_length = float(string_lengths.mean())
        
        # Numeric analysis
        if profile.detected_type in [DataType.INTEGER, DataType.FLOAT]:
            try:
                numeric_series = pd.to_numeric(non_null_series, errors='coerce')
                numeric_series = numeric_series.dropna()
                
                if len(numeric_series) > 0:
                    profile.min_value = float(numeric_series.min())
                    profile.max_value = float(numeric_series.max())
                    profile.mean_value = float(numeric_series.mean())
                    profile.std_value = float(numeric_series.std())
            except Exception:
                pass
        
        # Categorical analysis
        unique_ratio = profile.unique_values / profile.total_values
        if unique_ratio < 0.1 or profile.detected_type == DataType.CATEGORICAL:
            profile.is_categorical = True
            profile.categories = non_null_series.value_counts().head(20).index.tolist()
            profile.category_frequencies = non_null_series.value_counts().head(20).to_dict()
        
        # Calculate quality scores
        profile.completeness_score = (profile.total_values - profile.null_count) / profile.total_values
        profile.consistency_score = self._calculate_consistency_score(non_null_series, profile.detected_type)
        profile.validity_score = self._calculate_validity_score(non_null_series, profile.detected_type)
        profile.overall_quality_score = (profile.completeness_score + profile.consistency_score + profile.validity_score) / 3
        
        # Generate validation rules
        profile.validation_rules = self._generate_validation_rules(profile)
        
        return profile
    
    def _calculate_consistency_score(self, series: pd.Series, data_type: DataType) -> float:
        """Calculate consistency score based on pattern matching"""
        if len(series) == 0:
            return 0.0
        
        if data_type in self.patterns:
            patterns = self.patterns[data_type]
            consistent_count = 0
            
            for value in series.astype(str):
                for pattern in patterns:
                    if re.match(pattern, value, re.IGNORECASE):
                        consistent_count += 1
                        break
            
            return consistent_count / len(series)
        
        return 1.0  # No specific pattern to check
    
    def _calculate_validity_score(self, series: pd.Series, data_type: DataType) -> float:
        """Calculate validity score based on data type conversion success"""
        if len(series) == 0:
            return 0.0
        
        valid_count = 0
        
        if data_type == DataType.INTEGER:
            for value in series:
                try:
                    int(value)
                    valid_count += 1
                except (ValueError, TypeError):
                    pass
        elif data_type == DataType.FLOAT:
            for value in series:
                try:
                    float(value)
                    valid_count += 1
                except (ValueError, TypeError):
                    pass
        elif data_type == DataType.BOOLEAN:
            for value in series:
                if self._is_boolean(value):
                    valid_count += 1
        elif data_type in [DataType.DATE, DataType.DATETIME]:
            for value in series:
                if self._is_date(value) or self._is_datetime(value):
                    valid_count += 1
        else:
            valid_count = len(series)  # Assume all string types are valid
        
        return valid_count / len(series)
    
    def _generate_validation_rules(self, profile: FieldProfile) -> Dict[str, Any]:
        """Generate validation rules based on field profile"""
        rules = {
            'type': profile.detected_type.value,
            'nullable': profile.nullable,
            'required': not profile.nullable
        }
        
        # Add length constraints for string types
        if profile.min_length is not None and profile.max_length is not None:
            rules['min_length'] = max(1, profile.min_length - 2)  # Allow some flexibility
            rules['max_length'] = profile.max_length + 10  # Allow some flexibility
        
        # Add range constraints for numeric types
        if profile.min_value is not None and profile.max_value is not None:
            # Add some tolerance based on standard deviation
            tolerance = profile.std_value * 2 if profile.std_value else 0
            rules['min_value'] = profile.min_value - tolerance
            rules['max_value'] = profile.max_value + tolerance
        
        # Add categorical constraints
        if profile.is_categorical and profile.categories:
            rules['allowed_values'] = profile.categories
            rules['categorical'] = True
        
        # Add pattern constraints
        if profile.detected_type in self.patterns:
            rules['patterns'] = self.patterns[profile.detected_type]
        
        return rules
    
    def infer_schema(self, df: pd.DataFrame) -> Dict[str, FieldProfile]:
        """Infer schema for entire DataFrame"""
        logger.info(f"Inferring schema for DataFrame with {len(df.columns)} columns and {len(df)} rows")
        
        schema = {}
        
        for column in df.columns:
            logger.info(f"Profiling column: {column}")
            profile = self.profile_field(df[column], column)
            schema[column] = profile
            
            logger.info(f"  Detected type: {profile.detected_type}")
            logger.info(f"  Quality score: {profile.overall_quality_score:.2f}")
            logger.info(f"  Completeness: {profile.completeness_score:.2f}")
        
        return schema


class DynamicDataValidator:
    """Dynamic data validator that uses inferred schema"""
    
    def __init__(self, schema: Dict[str, FieldProfile] = None):
        self.schema = schema or {}
        self.inference_engine = IntelligentSchemaInference()
    
    def learn_from_data(self, df: pd.DataFrame) -> Dict[str, FieldProfile]:
        """Learn schema from sample data"""
        logger.info("Learning schema from data...")
        self.schema = self.inference_engine.infer_schema(df)
        return self.schema
    
    def validate_record(self, record: Dict[str, Any], record_id: int = 0) -> ValidationResult:
        """Validate a single record against the learned schema"""
        result = ValidationResult(record_id=record_id)
        field_scores = {}
        
        for field_name, profile in self.schema.items():
            field_score = 1.0
            
            # Check if field exists
            if field_name not in record:
                if not profile.nullable:
                    result.issues.append((field_name, QualityIssue.MISSING_VALUE, "Required field is missing"))
                    field_score = 0.0
                else:
                    result.warnings.append(f"Optional field '{field_name}' is missing")
                    field_score = 0.8
            else:
                value = record[field_name]
                
                # Check for null values
                if pd.isna(value) or value is None or value == '':
                    if not profile.nullable:
                        result.issues.append((field_name, QualityIssue.MISSING_VALUE, "Field cannot be null"))
                        field_score = 0.0
                    else:
                        field_score = 0.9
                else:
                    # Validate against rules
                    field_score = self._validate_field_value(field_name, value, profile, result)
            
            field_scores[field_name] = field_score
        
        # Check for unexpected fields
        for field_name in record.keys():
            if field_name not in self.schema:
                result.warnings.append(f"Unexpected field '{field_name}' found")
        
        # Calculate overall scores
        result.field_scores = field_scores
        result.quality_score = sum(field_scores.values()) / len(field_scores) if field_scores else 0.0
        result.is_valid = len(result.issues) == 0 and result.quality_score >= 0.6
        
        return result
    
    def _validate_field_value(self, field_name: str, value: Any, profile: FieldProfile, result: ValidationResult) -> float:
        """Validate a field value against its profile"""
        score = 1.0
        rules = profile.validation_rules
        
        # Type validation
        if not self._validate_type(value, profile.detected_type):
            result.issues.append((field_name, QualityIssue.INVALID_FORMAT, f"Invalid {profile.detected_type.value} format"))
            score *= 0.5
        
        # Length validation
        if 'min_length' in rules and 'max_length' in rules:
            value_length = len(str(value))
            if value_length < rules['min_length']:
                result.issues.append((field_name, QualityIssue.LENGTH_VIOLATION, f"Value too short (min: {rules['min_length']})"))
                score *= 0.7
            elif value_length > rules['max_length']:
                result.issues.append((field_name, QualityIssue.LENGTH_VIOLATION, f"Value too long (max: {rules['max_length']})"))
                score *= 0.8
        
        # Range validation
        if profile.detected_type in [DataType.INTEGER, DataType.FLOAT]:
            try:
                numeric_value = float(value)
                if 'min_value' in rules and numeric_value < rules['min_value']:
                    result.issues.append((field_name, QualityIssue.OUT_OF_RANGE, f"Value below minimum ({rules['min_value']})"))
                    score *= 0.7
                elif 'max_value' in rules and numeric_value > rules['max_value']:
                    result.issues.append((field_name, QualityIssue.OUT_OF_RANGE, f"Value above maximum ({rules['max_value']})"))
                    score *= 0.7
            except (ValueError, TypeError):
                pass
        
        # Categorical validation
        if rules.get('categorical') and 'allowed_values' in rules:
            if value not in rules['allowed_values']:
                result.warnings.append(f"Field '{field_name}' has unexpected category: {value}")
                score *= 0.9
        
        # Pattern validation
        if 'patterns' in rules:
            pattern_match = False
            for pattern in rules['patterns']:
                if re.match(pattern, str(value), re.IGNORECASE):
                    pattern_match = True
                    break
            
            if not pattern_match:
                result.issues.append((field_name, QualityIssue.INCONSISTENT_PATTERN, f"Value doesn't match expected pattern"))
                score *= 0.6
        
        return score
    
    def _validate_type(self, value: Any, expected_type: DataType) -> bool:
        """Validate value against expected data type"""
        if expected_type == DataType.INTEGER:
            return self.inference_engine._is_integer(value)
        elif expected_type == DataType.FLOAT:
            return self.inference_engine._is_float(value)
        elif expected_type == DataType.BOOLEAN:
            return self.inference_engine._is_boolean(value)
        elif expected_type == DataType.DATE:
            return self.inference_engine._is_date(value)
        elif expected_type == DataType.DATETIME:
            return self.inference_engine._is_datetime(value)
        elif expected_type == DataType.JSON:
            return self.inference_engine._is_json(value)
        elif expected_type in self.inference_engine.patterns:
            patterns = self.inference_engine.patterns[expected_type]
            return any(re.match(pattern, str(value), re.IGNORECASE) for pattern in patterns)
        else:
            return True  # String types and others are generally valid
    
    def validate_batch(self, records: List[Dict[str, Any]]) -> List[ValidationResult]:
        """Validate a batch of records"""
        logger.info(f"Validating batch of {len(records)} records")
        
        results = []
        for i, record in enumerate(records):
            result = self.validate_record(record, record_id=i)
            results.append(result)
            
            if (i + 1) % 100 == 0:
                logger.info(f"Validated {i + 1}/{len(records)} records")
        
        valid_count = sum(1 for r in results if r.is_valid)
        logger.info(f"Validation completed: {valid_count}/{len(results)} valid records")
        
        return results
    
    def get_schema_summary(self) -> Dict[str, Any]:
        """Get a summary of the learned schema"""
        summary = {
            'total_fields': len(self.schema),
            'field_types': {},
            'quality_distribution': {},
            'fields': {}
        }
        
        # Count field types
        for profile in self.schema.values():
            type_name = profile.detected_type.value
            summary['field_types'][type_name] = summary['field_types'].get(type_name, 0) + 1
        
        # Quality distribution
        quality_ranges = {'excellent': 0, 'good': 0, 'fair': 0, 'poor': 0}
        for profile in self.schema.values():
            if profile.overall_quality_score >= 0.9:
                quality_ranges['excellent'] += 1
            elif profile.overall_quality_score >= 0.8:
                quality_ranges['good'] += 1
            elif profile.overall_quality_score >= 0.6:
                quality_ranges['fair'] += 1
            else:
                quality_ranges['poor'] += 1
        
        summary['quality_distribution'] = quality_ranges
        
        # Field details
        for name, profile in self.schema.items():
            summary['fields'][name] = {
                'type': profile.detected_type.value,
                'nullable': profile.nullable,
                'unique_values': profile.unique_values,
                'completeness': profile.completeness_score,
                'quality_score': profile.overall_quality_score,
                'sample_values': profile.sample_values[:3]  # First 3 samples
            }
        
        return summary
