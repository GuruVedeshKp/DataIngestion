"""
Pydantic models for data validation and processing.
Supports multiple data schemas and validation rules.
"""

from pydantic import BaseModel, Field, validator, ValidationError
from typing import Optional, Union, List, Dict, Any
from datetime import datetime
from enum import Enum
import re


class DataQualityLevel(str, Enum):
    """Data quality levels for classification"""
    EXCELLENT = "excellent"
    GOOD = "good"
    FAIR = "fair"
    POOR = "poor"


class CustomerTransaction(BaseModel):
    """Enhanced Customer Transaction model with comprehensive validation"""
    
    customer_id: str = Field(..., min_length=1, max_length=10, description="Unique customer identifier")
    company_name: str = Field(..., min_length=1, max_length=200, description="Company name")
    contact_name: str = Field(..., min_length=1, max_length=100, description="Contact person name")
    contact_title: str = Field(..., min_length=1, max_length=100, description="Contact person title")
    city: str = Field(..., min_length=1, max_length=50, description="City name")
    region: Optional[str] = Field(None, max_length=50, description="Region/State")
    postal_code: str = Field(..., min_length=1, max_length=20, description="Postal/ZIP code")
    country: str = Field(..., min_length=2, max_length=50, description="Country name")
    segment: str = Field(..., description="Business segment")
    metro_area: Union[bool, str] = Field(..., description="Metro area indicator")
    
    # Additional metadata fields
    data_quality_score: Optional[float] = Field(None, ge=0.0, le=1.0, description="Data quality score")
    validation_timestamp: Optional[datetime] = Field(default_factory=datetime.now, description="Validation timestamp")
    source_file: Optional[str] = Field(None, description="Source file name")
    
    class Config:
        # Allow field name variations for compatibility
        populate_by_name = True
        field_aliases = {
            'customer_id': 'CustomerID',
            'company_name': 'CompanyName',
            'contact_name': 'ContactName',
            'contact_title': 'ContactTitle',
            'city': 'City',
            'region': 'Region',
            'postal_code': 'PostalCode',
            'country': 'Country',
            'segment': 'Segment',
            'metro_area': 'MetroArea'
        }
    
    @validator('customer_id')
    def validate_customer_id(cls, v):
        """Validate customer ID format"""
        if not v or not isinstance(v, str):
            raise ValueError('Customer ID must be a non-empty string')
        # Remove any whitespace
        v = v.strip().upper()
        if len(v) < 1:
            raise ValueError('Customer ID cannot be empty after trimming')
        return v
    
    @validator('contact_name', 'company_name')
    def validate_names(cls, v):
        """Validate name fields"""
        if not v or not isinstance(v, str):
            raise ValueError('Name fields must be non-empty strings')
        v = v.strip()
        if len(v) < 1:
            raise ValueError('Name cannot be empty after trimming')
        return v
    
    @validator('postal_code')
    def validate_postal_code(cls, v):
        """Validate postal code format"""
        if not v:
            raise ValueError('Postal code is required')
        v = str(v).strip()
        # Basic postal code validation (can be enhanced for specific countries)
        if len(v) < 3:
            raise ValueError('Postal code must be at least 3 characters')
        return v
    
    @validator('metro_area', pre=True)
    def validate_metro_area(cls, v):
        """Convert metro area to boolean if string"""
        if isinstance(v, str):
            v_lower = v.lower().strip()
            if v_lower in ['true', 'yes', '1', 'y']:
                return True
            elif v_lower in ['false', 'no', '0', 'n']:
                return False
            else:
                return v  # Keep as string if not boolean-like
        return v
    
    @validator('segment')
    def validate_segment(cls, v):
        """Validate business segment"""
        if not v:
            raise ValueError('Segment is required')
        v = str(v).strip().title()  # Normalize capitalization
        valid_segments = ['Healthcare', 'Government', 'Corporate', 'Hospitality', 'Retail', 'Education', 'Finance']
        if v not in valid_segments:
            # Don't reject, but flag for review
            pass
        return v
    
    def calculate_quality_score(self) -> float:
        """Calculate data quality score based on completeness and validity"""
        score = 0.0
        total_checks = 10
        
        # Required field completeness (5 points)
        required_fields = [self.customer_id, self.company_name, self.contact_name, 
                          self.city, self.postal_code, self.country]
        score += sum(1 for field in required_fields if field and len(str(field).strip()) > 0)
        
        # Optional field completeness (2 points)
        if self.region and len(str(self.region).strip()) > 0:
            score += 1
        if self.contact_title and len(str(self.contact_title).strip()) > 0:
            score += 1
        
        # Data format validity (2 points)
        if len(self.customer_id) >= 3:  # Reasonable customer ID length
            score += 1
        if len(self.postal_code) >= 5:  # Reasonable postal code length
            score += 1
        
        # Business logic validity (1 point)
        valid_segments = ['Healthcare', 'Government', 'Corporate', 'Hospitality', 'Retail', 'Education', 'Finance']
        if self.segment in valid_segments:
            score += 1
        
        return score / total_checks


class DataValidationResult(BaseModel):
    """Result of data validation process"""
    
    is_valid: bool
    original_data: Dict[str, Any]
    validated_data: Optional[CustomerTransaction] = None
    errors: List[str] = []
    warnings: List[str] = []
    quality_score: Optional[float] = None
    quality_level: Optional[DataQualityLevel] = None
    processing_timestamp: datetime = Field(default_factory=datetime.now)
    
    def add_error(self, error_msg: str):
        """Add validation error"""
        self.errors.append(error_msg)
        self.is_valid = False
    
    def add_warning(self, warning_msg: str):
        """Add validation warning"""
        self.warnings.append(warning_msg)
    
    def set_quality_level(self):
        """Set quality level based on score"""
        if self.quality_score is None:
            return
        
        if self.quality_score >= 0.9:
            self.quality_level = DataQualityLevel.EXCELLENT
        elif self.quality_score >= 0.8:
            self.quality_level = DataQualityLevel.GOOD
        elif self.quality_score >= 0.6:
            self.quality_level = DataQualityLevel.FAIR
        else:
            self.quality_level = DataQualityLevel.POOR


class DataIngestionStats(BaseModel):
    """Statistics for data ingestion process"""
    
    total_records: int = 0
    valid_records: int = 0
    invalid_records: int = 0
    excellent_quality: int = 0
    good_quality: int = 0
    fair_quality: int = 0
    poor_quality: int = 0
    processing_start_time: datetime = Field(default_factory=datetime.now)
    processing_end_time: Optional[datetime] = None
    source_files: List[str] = []
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate percentage"""
        if self.total_records == 0:
            return 0.0
        return (self.valid_records / self.total_records) * 100
    
    @property
    def processing_duration(self) -> Optional[float]:
        """Calculate processing duration in seconds"""
        if self.processing_end_time:
            return (self.processing_end_time - self.processing_start_time).total_seconds()
        return None
    
    def update_quality_stats(self, quality_level: DataQualityLevel):
        """Update quality statistics"""
        if quality_level == DataQualityLevel.EXCELLENT:
            self.excellent_quality += 1
        elif quality_level == DataQualityLevel.GOOD:
            self.good_quality += 1
        elif quality_level == DataQualityLevel.FAIR:
            self.fair_quality += 1
        elif quality_level == DataQualityLevel.POOR:
            self.poor_quality += 1
