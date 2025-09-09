"""
Enhanced data validation service with intelligent schema inference.
Handles dynamic validation for any data structure.
"""

from typing import Dict, Any, List, Tuple
import logging
from datetime import datetime
import pandas as pd

from models.data_models import DataIngestionStats, DataQualityLevel
from services.intelligent_validator import DynamicDataValidator, ValidationResult as IntelligentValidationResult

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class EnhancedValidationResult:
    """Enhanced validation result that wraps intelligent validation"""
    
    def __init__(self, intelligent_result: IntelligentValidationResult, original_data: Dict[str, Any]):
        self.is_valid = intelligent_result.is_valid
        self.original_data = original_data
        self.validated_data = original_data if intelligent_result.is_valid else None
        self.errors = []
        self.warnings = intelligent_result.warnings.copy()
        self.quality_score = intelligent_result.quality_score
        self.quality_level = self._determine_quality_level(intelligent_result.quality_score)
        self.processing_timestamp = datetime.now()
        self.field_scores = intelligent_result.field_scores
        
        # Convert issues to error messages
        for field_name, issue_type, description in intelligent_result.issues:
            self.errors.append(f"Field '{field_name}': {description}")
    
    def _determine_quality_level(self, score: float) -> DataQualityLevel:
        """Determine quality level based on score"""
        if score >= 0.9:
            return DataQualityLevel.EXCELLENT
        elif score >= 0.8:
            return DataQualityLevel.GOOD
        elif score >= 0.6:
            return DataQualityLevel.FAIR
        else:
            return DataQualityLevel.POOR
    
    def add_error(self, error_msg: str):
        """Add validation error"""
        self.errors.append(error_msg)
        self.is_valid = False
    
    def add_warning(self, warning_msg: str):
        """Add validation warning"""
        self.warnings.append(warning_msg)


class DataValidator:
    """Enhanced service for intelligent data validation"""
    
    def __init__(self, auto_learn_schema: bool = True):
        self.stats = DataIngestionStats()
        self.validation_results: List[EnhancedValidationResult] = []
        self.dynamic_validator = DynamicDataValidator()
        self.auto_learn_schema = auto_learn_schema
        self.schema_learned = False
        
        logger.info("Enhanced Data Validator initialized with intelligent schema inference")
    
    def learn_schema_from_sample(self, sample_data: List[Dict[str, Any]], sample_size: int = 1000):
        """Learn schema from a sample of the data"""
        if not sample_data:
            logger.warning("No sample data provided for schema learning")
            return
        
        # Take a sample for schema learning
        sample = sample_data[:min(sample_size, len(sample_data))]
        df_sample = pd.DataFrame(sample)
        
        logger.info(f"Learning schema from {len(sample)} sample records")
        schema = self.dynamic_validator.learn_from_data(df_sample)
        self.schema_learned = True
        
        # Log schema summary
        summary = self.dynamic_validator.get_schema_summary()
        logger.info(f"Schema learned: {summary['total_fields']} fields detected")
        logger.info(f"Field types: {summary['field_types']}")
        logger.info(f"Quality distribution: {summary['quality_distribution']}")
        
        return schema
    
    def validate_record(self, data: Dict[str, Any], source_file: str = None) -> EnhancedValidationResult:
        """
        Validate a single data record using intelligent validation
        
        Args:
            data: Raw data dictionary
            source_file: Source file name for tracking
            
        Returns:
            EnhancedValidationResult: Validation result with quality metrics
        """
        # Learn schema if not already learned and auto-learning is enabled
        if not self.schema_learned and self.auto_learn_schema:
            logger.info("Auto-learning schema from first record")
            self.learn_schema_from_sample([data])
        
        try:
            # Add source file to data if provided
            if source_file:
                data['_source_file'] = source_file
            
            # Validate using intelligent validator
            intelligent_result = self.dynamic_validator.validate_record(data)
            
            # Wrap in enhanced result
            result = EnhancedValidationResult(intelligent_result, data)
            
            # Add quality-based warnings
            if result.quality_score < 0.8:
                result.add_warning(f"Data quality score is {result.quality_score:.2f} - consider reviewing")
            
            if result.quality_score < 0.6:
                result.add_warning("Poor data quality detected - manual review recommended")
            
            logger.debug(f"Validated record with quality score: {result.quality_score:.2f}")
            
        except Exception as e:
            # Create error result for failed validation
            result = EnhancedValidationResult(
                IntelligentValidationResult(record_id=0, is_valid=False, quality_score=0.0),
                data
            )
            result.add_error(f"Validation error: {str(e)}")
            logger.error(f"Validation error: {str(e)}")
        
        # Update statistics
        self._update_stats(result)
        self.validation_results.append(result)
        
        return result
    
    def validate_batch(self, data_list: List[Dict[str, Any]], source_file: str = None) -> List[EnhancedValidationResult]:
        """
        Validate a batch of data records with intelligent schema learning
        
        Args:
            data_list: List of raw data dictionaries
            source_file: Source file name for tracking
            
        Returns:
            List[EnhancedValidationResult]: List of validation results
        """
        logger.info(f"Starting intelligent batch validation of {len(data_list)} records")
        
        # Learn schema from sample if not already learned
        if not self.schema_learned and self.auto_learn_schema:
            self.learn_schema_from_sample(data_list, sample_size=min(1000, len(data_list)))
        
        batch_results = []
        
        try:
            # Validate all records using intelligent validator
            intelligent_results = self.dynamic_validator.validate_batch(data_list)
            
            # Wrap results
            for i, (data, intelligent_result) in enumerate(zip(data_list, intelligent_results)):
                try:
                    if source_file:
                        data['_source_file'] = source_file
                    
                    result = EnhancedValidationResult(intelligent_result, data)
                    batch_results.append(result)
                    
                    # Update statistics
                    self._update_stats(result)
                    
                    # Log progress for large batches
                    if (i + 1) % 100 == 0:
                        logger.info(f"Processed {i + 1}/{len(data_list)} records")
                        
                except Exception as e:
                    logger.error(f"Error processing record {i}: {str(e)}")
                    # Create error result for failed record
                    error_result = EnhancedValidationResult(
                        IntelligentValidationResult(record_id=i, is_valid=False, quality_score=0.0),
                        data
                    )
                    error_result.add_error(f"Processing error: {str(e)}")
                    batch_results.append(error_result)
                    self._update_stats(error_result)
        
        except Exception as e:
            logger.error(f"Batch validation failed: {str(e)}")
            raise
        
        # Store results
        self.validation_results.extend(batch_results)
        
        logger.info(f"Batch validation completed. Valid: {self.stats.valid_records}, Invalid: {self.stats.invalid_records}")
        return batch_results
    
    def get_good_data(self) -> List[Dict[str, Any]]:
        """Get all successfully validated records"""
        return [result.validated_data for result in self.validation_results 
                if result.is_valid and result.validated_data is not None]
    
    def get_bad_data(self) -> List[EnhancedValidationResult]:
        """Get all validation results for invalid records"""
        return [result for result in self.validation_results if not result.is_valid]
    
    def get_quality_filtered_data(self, min_quality_score: float = 0.8) -> Tuple[List[Dict[str, Any]], List[EnhancedValidationResult]]:
        """
        Split data based on quality score threshold
        
        Args:
            min_quality_score: Minimum quality score for 'good' data
            
        Returns:
            Tuple of (high_quality_data, low_quality_data)
        """
        high_quality = []
        low_quality = []
        
        for result in self.validation_results:
            if result.is_valid and result.quality_score >= min_quality_score:
                high_quality.append(result.validated_data)
            else:
                low_quality.append(result)
        
        return high_quality, low_quality
    
    def _update_stats(self, result: EnhancedValidationResult):
        """Update internal statistics"""
        self.stats.total_records += 1
        
        if result.is_valid:
            self.stats.valid_records += 1
            if result.quality_level:
                self.stats.update_quality_stats(result.quality_level)
        else:
            self.stats.invalid_records += 1
    
    def get_statistics(self) -> DataIngestionStats:
        """Get current processing statistics"""
        # Update end time
        self.stats.processing_end_time = datetime.now()
        return self.stats
    
    def reset_stats(self):
        """Reset statistics and validation results"""
        self.stats = DataIngestionStats()
        self.validation_results = []
        self.schema_learned = False
        self.dynamic_validator = DynamicDataValidator()
        logger.info("Statistics and validation results reset")
    
    def export_validation_report(self) -> Dict[str, Any]:
        """Export comprehensive validation report"""
        good_data = self.get_good_data()
        bad_data = self.get_bad_data()
        
        report = {
            "summary": {
                "total_records": self.stats.total_records,
                "valid_records": self.stats.valid_records,
                "invalid_records": self.stats.invalid_records,
                "success_rate": f"{self.stats.success_rate:.2f}%",
                "processing_duration": self.stats.processing_duration,
                "schema_learned": self.schema_learned
            },
            "quality_distribution": {
                "excellent": self.stats.excellent_quality,
                "good": self.stats.good_quality,
                "fair": self.stats.fair_quality,
                "poor": self.stats.poor_quality
            },
            "schema_summary": self.dynamic_validator.get_schema_summary() if self.schema_learned else None,
            "validation_errors": [],
            "data_quality_insights": self._generate_quality_insights()
        }
        
        # Add detailed error information
        for result in bad_data[:50]:  # Limit to first 50 errors
            report["validation_errors"].append({
                "original_data": result.original_data,
                "errors": result.errors,
                "warnings": result.warnings,
                "quality_score": result.quality_score,
                "field_scores": result.field_scores,
                "timestamp": result.processing_timestamp.isoformat()
            })
        
        return report
    
    def _generate_quality_insights(self) -> List[str]:
        """Generate insights about data quality patterns"""
        insights = []
        
        if self.stats.total_records == 0:
            return ["No data processed yet"]
        
        # Schema insights
        if self.schema_learned:
            schema_summary = self.dynamic_validator.get_schema_summary()
            insights.append(f"Automatically detected {schema_summary['total_fields']} fields with {len(schema_summary['field_types'])} different data types")
            
            # Most common data types
            if schema_summary['field_types']:
                most_common_type = max(schema_summary['field_types'].items(), key=lambda x: x[1])
                insights.append(f"Most common data type: {most_common_type[0]} ({most_common_type[1]} fields)")
        
        # Success rate insights
        if self.stats.success_rate >= 95:
            insights.append("Excellent data quality - very high success rate with intelligent validation")
        elif self.stats.success_rate >= 80:
            insights.append("Good data quality - acceptable success rate")
        elif self.stats.success_rate >= 60:
            insights.append("Fair data quality - consider data source review")
        else:
            insights.append("Poor data quality - immediate attention required")
        
        # Quality distribution insights
        total_valid = self.stats.valid_records
        if total_valid > 0:
            excellent_pct = (self.stats.excellent_quality / total_valid) * 100
            poor_pct = (self.stats.poor_quality / total_valid) * 100
            
            if excellent_pct >= 80:
                insights.append("Majority of valid records have excellent quality scores")
            elif poor_pct >= 30:
                insights.append("High percentage of poor quality records among valid data")
        
        # Field-specific insights
        if hasattr(self, 'validation_results') and self.validation_results:
            # Analyze common field issues
            field_error_counts = {}
            for result in self.validation_results:
                if not result.is_valid:
                    for error in result.errors:
                        if "Field '" in error:
                            field_name = error.split("Field '")[1].split("'")[0]
                            field_error_counts[field_name] = field_error_counts.get(field_name, 0) + 1
            
            if field_error_counts:
                problematic_field = max(field_error_counts.items(), key=lambda x: x[1])
                insights.append(f"Most problematic field: '{problematic_field[0]}' with {problematic_field[1]} validation issues")
        
        return insights
    
    def get_schema_info(self) -> Dict[str, Any]:
        """Get information about the learned schema"""
        if not self.schema_learned:
            return {"status": "No schema learned yet"}
        
        return {
            "status": "Schema learned",
            "summary": self.dynamic_validator.get_schema_summary(),
            "total_fields": len(self.dynamic_validator.schema),
            "field_details": {
                name: {
                    "type": profile.detected_type.value,
                    "nullable": profile.nullable,
                    "quality_score": profile.overall_quality_score,
                    "sample_values": profile.sample_values[:3]
                }
                for name, profile in self.dynamic_validator.schema.items()
            }
        }
