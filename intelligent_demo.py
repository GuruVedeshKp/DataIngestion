"""
Demo script showing intelligent data validation capabilities.
Demonstrates how the system automatically learns and validates any data structure.
"""

import pandas as pd
import json
import sys
from pathlib import Path

# Add the project root to the path
sys.path.append(str(Path(__file__).parent))

from services.intelligent_validator import IntelligentSchemaInference, DynamicDataValidator
from services.data_validator import DataValidator
from services.file_processor import FileProcessor


def demo_schema_inference():
    """Demonstrate automatic schema inference"""
    print("=" * 80)
    print("🧠 INTELLIGENT SCHEMA INFERENCE DEMO")
    print("=" * 80)
    
    # Load sample data
    processor = FileProcessor()
    df = processor.load_data_from_file("CustomersData.csv")
    
    print(f"📊 Loaded sample data: {len(df)} records with {len(df.columns)} columns")
    print(f"🔤 Columns: {list(df.columns)}")
    print()
    
    # Initialize schema inference engine
    inference_engine = IntelligentSchemaInference()
    
    print("🔍 Analyzing data and inferring schema...")
    schema = inference_engine.infer_schema(df)
    
    print()
    print("📋 INFERRED SCHEMA SUMMARY:")
    print("-" * 50)
    
    for field_name, profile in schema.items():
        print(f"📌 Field: {field_name}")
        print(f"   Type: {profile.detected_type.value}")
        print(f"   Nullable: {profile.nullable}")
        print(f"   Quality Score: {profile.overall_quality_score:.2f}")
        print(f"   Completeness: {profile.completeness_score:.2f}")
        print(f"   Sample Values: {profile.sample_values[:3]}")
        
        if profile.is_categorical:
            print(f"   Categories: {profile.categories[:5]}")
        
        if profile.min_length is not None:
            print(f"   Length Range: {profile.min_length}-{profile.max_length}")
        
        if profile.min_value is not None:
            print(f"   Value Range: {profile.min_value:.2f}-{profile.max_value:.2f}")
        
        print()


def demo_dynamic_validation():
    """Demonstrate dynamic validation with learned schema"""
    print("=" * 80)
    print("🎯 DYNAMIC VALIDATION DEMO")
    print("=" * 80)
    
    # Load sample data
    processor = FileProcessor()
    df = processor.load_data_from_file("CustomersData.csv")
    records = processor.convert_dataframe_to_records(df)
    
    # Initialize dynamic validator
    validator = DynamicDataValidator()
    
    print("📚 Learning schema from data...")
    schema = validator.learn_from_data(df)
    
    print("✅ Schema learned! Now validating records...")
    
    # Validate some records
    sample_records = records[:5]
    results = validator.validate_batch(sample_records)
    
    print()
    print("📊 VALIDATION RESULTS:")
    print("-" * 50)
    
    for i, result in enumerate(results):
        print(f"Record {i+1}:")
        print(f"   ✅ Valid: {result.is_valid}")
        print(f"   🎯 Quality Score: {result.quality_score:.2f}")
        print(f"   📈 Field Scores: {result.field_scores}")
        
        if result.issues:
            print(f"   ⚠️ Issues: {len(result.issues)}")
            for field, issue_type, description in result.issues:
                print(f"      - {field}: {description}")
        
        if result.warnings:
            print(f"   💡 Warnings: {result.warnings}")
        
        print()


def demo_enhanced_pipeline():
    """Demonstrate the enhanced validation pipeline"""
    print("=" * 80)
    print("🚀 ENHANCED VALIDATION PIPELINE DEMO")
    print("=" * 80)
    
    # Initialize enhanced validator
    validator = DataValidator(auto_learn_schema=True)
    
    # Load and process data
    processor = FileProcessor()
    df = processor.load_data_from_file("CustomersData.csv")
    records = processor.convert_dataframe_to_records(df)
    
    print(f"📂 Processing {len(records)} records with intelligent validation...")
    
    # Validate batch - schema will be learned automatically
    results = validator.validate_batch(records, source_file="CustomersData.csv")
    
    # Get statistics
    stats = validator.get_statistics()
    
    print()
    print("📊 PROCESSING SUMMARY:")
    print("-" * 40)
    print(f"Total Records: {stats.total_records}")
    print(f"Valid Records: {stats.valid_records}")
    print(f"Invalid Records: {stats.invalid_records}")
    print(f"Success Rate: {stats.success_rate:.2f}%")
    print()
    
    print("🎯 QUALITY DISTRIBUTION:")
    print("-" * 40)
    print(f"Excellent: {stats.excellent_quality}")
    print(f"Good: {stats.good_quality}")
    print(f"Fair: {stats.fair_quality}")
    print(f"Poor: {stats.poor_quality}")
    print()
    
    # Get schema information
    schema_info = validator.get_schema_info()
    if schema_info['status'] == 'Schema learned':
        print("🧠 LEARNED SCHEMA:")
        print("-" * 40)
        summary = schema_info['summary']
        print(f"Total Fields: {summary['total_fields']}")
        print(f"Field Types: {summary['field_types']}")
        print(f"Quality Distribution: {summary['quality_distribution']}")
        print()
    
    # Export detailed report
    report = validator.export_validation_report()
    
    print("💡 DATA QUALITY INSIGHTS:")
    print("-" * 40)
    for insight in report['data_quality_insights']:
        print(f"• {insight}")
    print()
    
    # Show a few validation errors if any
    if report['validation_errors']:
        print("⚠️ SAMPLE VALIDATION ISSUES:")
        print("-" * 40)
        for error in report['validation_errors'][:3]:  # Show first 3 errors
            print(f"Quality Score: {error['quality_score']:.2f}")
            print(f"Errors: {error['errors']}")
            print(f"Field Scores: {error['field_scores']}")
            print()


def demo_different_data_types():
    """Demonstrate validation with different data types"""
    print("=" * 80)
    print("🔬 DIFFERENT DATA TYPES DEMO")
    print("=" * 80)
    
    # Create sample data with various data types
    sample_data = [
        {
            "user_id": "USR001",
            "email": "john.doe@example.com",
            "phone": "+1-555-123-4567",
            "age": 25,
            "salary": 75000.50,
            "is_active": True,
            "join_date": "2023-01-15",
            "department": "Engineering",
            "address": "123 Main St, Anytown, USA 12345",
            "website": "https://johndoe.portfolio.com"
        },
        {
            "user_id": "USR002",
            "email": "jane.smith@company.org",
            "phone": "(555) 987-6543",
            "age": 32,
            "salary": 85000.00,
            "is_active": False,
            "join_date": "2022-03-20",
            "department": "Marketing",
            "address": "456 Oak Ave, Somewhere, CA 90210",
            "website": "https://janesmith.com"
        },
        {
            "user_id": "USR003",
            "email": "invalid-email",  # Invalid email
            "phone": "123",  # Invalid phone
            "age": "thirty",  # Invalid age (string instead of number)
            "salary": None,  # Missing salary
            "is_active": "maybe",  # Invalid boolean
            "join_date": "2023-13-45",  # Invalid date
            "department": "Unknown",
            "address": "",  # Empty address
            "website": "not-a-url"  # Invalid URL
        }
    ]
    
    # Create DataFrame and validate
    df = pd.DataFrame(sample_data)
    
    # Initialize validator
    validator = DynamicDataValidator()
    
    print("📊 Sample data created with various data types and quality issues")
    print("🔍 Learning schema...")
    
    schema = validator.learn_from_data(df)
    
    print()
    print("📋 DETECTED SCHEMA:")
    print("-" * 40)
    
    schema_summary = validator.get_schema_summary()
    for field_name, field_info in schema_summary['fields'].items():
        print(f"• {field_name}: {field_info['type']} (Quality: {field_info['quality_score']:.2f})")
    
    print()
    print("🎯 VALIDATING RECORDS:")
    print("-" * 40)
    
    # Validate each record
    for i, record in enumerate(sample_data):
        result = validator.validate_record(record, record_id=i)
        
        print(f"Record {i+1}: {'✅ Valid' if result.is_valid else '❌ Invalid'} (Score: {result.quality_score:.2f})")
        
        if result.issues:
            for field, issue_type, description in result.issues:
                print(f"   ⚠️ {field}: {description}")
        
        if result.warnings:
            for warning in result.warnings:
                print(f"   💡 {warning}")
        
        print()


def main():
    """Run all demonstrations"""
    print("🎬 INTELLIGENT DATA VALIDATION DEMONSTRATION")
    print("=" * 80)
    print("This demo shows how the system automatically learns from any data")
    print("and creates intelligent validation rules without predefined schemas.")
    print()
    
    try:
        # Check if sample data exists
        if not Path("CustomersData.csv").exists():
            print("⚠️ Sample data file 'CustomersData.csv' not found!")
            print("Running demo with synthetic data only...")
            demo_different_data_types()
        else:
            demo_schema_inference()
            demo_dynamic_validation() 
            demo_enhanced_pipeline()
            demo_different_data_types()
        
        print("=" * 80)
        print("🎉 DEMONSTRATION COMPLETED!")
        print("The system successfully:")
        print("• 🧠 Automatically inferred data schemas")
        print("• 🎯 Validated data without predefined rules") 
        print("• 📊 Generated quality scores and insights")
        print("• ⚡ Handled various data types intelligently")
        print("• 🔍 Detected patterns and anomalies")
        print("=" * 80)
        
    except Exception as e:
        print(f"❌ Demo failed: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
