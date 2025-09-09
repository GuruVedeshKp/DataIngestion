"""
Example usage scripts for the enhanced data ingestion pipeline.
Demonstrates different ways to use the pipeline components.
"""

import pandas as pd
import sys
import logging
from pathlib import Path

# Add the project root to the path
sys.path.append(str(Path(__file__).parent))

from data_ingestion_pipeline import DataIngestionPipeline
from services.file_processor import FileProcessor
from services.kafka_producer_enhanced import send_csv_to_kafka
from services.kafka_consumer_enhanced import EnhancedKafkaConsumer

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def example_file_validation():
    """Example: Validate a file without using Kafka"""
    print("=" * 60)
    print("EXAMPLE 1: FILE VALIDATION ONLY")
    print("=" * 60)
    
    pipeline = DataIngestionPipeline()
    
    try:
        results = pipeline.validate_file_only(
            input_file="CustomersData.csv",
            output_dir="validation_output",
            quality_threshold=0.8
        )
        
        print(f"✅ Validation completed successfully!")
        print(f"📊 Total records: {results['total_records']}")
        print(f"✅ Valid records: {results['valid_records']}")
        print(f"❌ Invalid records: {results['invalid_records']}")
        print(f"🎯 Success rate: {results['success_rate']}")
        print(f"📄 Report: {results['report_path']}")
        
    except Exception as e:
        print(f"❌ Validation failed: {str(e)}")


def example_file_to_kafka():
    """Example: Send file data to Kafka with validation"""
    print("\n" + "=" * 60)
    print("EXAMPLE 2: FILE TO KAFKA PIPELINE")
    print("=" * 60)
    
    pipeline = DataIngestionPipeline()
    
    try:
        results = pipeline.run_file_to_kafka_pipeline(
            input_file="CustomersData.csv",
            topic="customer_data_topic",
            validate_before_send=True
        )
        
        print(f"✅ File to Kafka pipeline completed!")
        print(f"📂 Input file: {results['input_file']}")
        print(f"📡 Kafka topic: {results['topic']}")
        
        if 'file_processing' in results:
            fp = results['file_processing']
            print(f"📊 Records loaded: {fp['total_records_loaded']}")
        
        if 'validation' in results:
            vp = results['validation']
            print(f"✅ Valid records: {vp['valid_records']}")
            print(f"❌ Invalid records: {vp['invalid_records']}")
            print(f"🎯 Success rate: {vp['success_rate']}")
        
        if 'kafka_sending' in results:
            ks = results['kafka_sending']
            print(f"📤 Records sent: {ks['sent_records']}")
            print(f"⚡ Records/second: {ks['records_per_second']:.2f}")
        
    except Exception as e:
        print(f"❌ Pipeline failed: {str(e)}")


def example_kafka_to_analysis():
    """Example: Consume from Kafka and perform analysis"""
    print("\n" + "=" * 60)
    print("EXAMPLE 3: KAFKA TO ANALYSIS PIPELINE")
    print("=" * 60)
    
    pipeline = DataIngestionPipeline()
    
    try:
        results = pipeline.run_kafka_to_analysis_pipeline(
            topic="customer_data_topic",
            timeout_seconds=30,
            max_records=100,
            quality_threshold=0.8,
            output_dir="kafka_analysis_output"
        )
        
        print(f"✅ Kafka to analysis pipeline completed!")
        
        if 'processing_summary' in results:
            ps = results['processing_summary']
            print(f"📊 Total records: {ps['total_records']}")
            print(f"✅ Valid records: {ps['valid_records']}")
            print(f"🎯 Success rate: {ps['success_rate']}")
        
        if 'report_path' in results:
            print(f"📄 Report: {results['report_path']}")
        
    except Exception as e:
        print(f"❌ Pipeline failed: {str(e)}")


def example_full_pipeline():
    """Example: Complete pipeline from file to analysis"""
    print("\n" + "=" * 60)
    print("EXAMPLE 4: FULL PIPELINE")
    print("=" * 60)
    
    pipeline = DataIngestionPipeline()
    
    try:
        results = pipeline.run_full_pipeline(
            input_file="CustomersData.csv",
            topic="full_pipeline_topic",
            timeout_seconds=30,
            max_records=100,
            quality_threshold=0.8,
            output_dir="full_pipeline_output"
        )
        
        print(f"✅ Full pipeline completed successfully!")
        print(f"📄 Final report: {results['final_report']}")
        print(f"📁 Output directory: {results['output_directory']}")
        
    except Exception as e:
        print(f"❌ Full pipeline failed: {str(e)}")


def example_file_processor():
    """Example: Using file processor directly"""
    print("\n" + "=" * 60)
    print("EXAMPLE 5: FILE PROCESSOR USAGE")
    print("=" * 60)
    
    processor = FileProcessor()
    
    # Show supported formats
    print("📋 Supported file formats:")
    for ext, desc in processor.get_supported_formats().items():
        print(f"  • .{ext}: {desc}")
    
    try:
        # Load the sample CSV
        df = processor.load_data_from_file("CustomersData.csv")
        print(f"\n📊 Loaded {len(df)} records from CSV")
        print(f"🔤 Columns: {list(df.columns)}")
        
        # Convert to records
        records = processor.convert_dataframe_to_records(df, clean_data=True)
        print(f"📝 Converted to {len(records)} record dictionaries")
        
        # Save in different formats
        output_dir = Path("file_processor_output")
        output_dir.mkdir(exist_ok=True)
        
        # Save as JSON
        processor.save_dataframe(df, str(output_dir / "sample.json"), "json")
        print(f"💾 Saved as JSON: {output_dir / 'sample.json'}")
        
        # Save as Parquet (if available)
        try:
            processor.save_dataframe(df, str(output_dir / "sample.parquet"), "parquet")
            print(f"💾 Saved as Parquet: {output_dir / 'sample.parquet'}")
        except Exception:
            print("⚠️ Parquet format not available (install pyarrow)")
        
    except Exception as e:
        print(f"❌ File processing failed: {str(e)}")


def example_quick_kafka_send():
    """Example: Quick way to send CSV to Kafka"""
    print("\n" + "=" * 60)
    print("EXAMPLE 6: QUICK KAFKA SEND")
    print("=" * 60)
    
    try:
        stats = send_csv_to_kafka(
            csv_file="CustomersData.csv",
            topic="quick_send_topic"
        )
        
        print(f"✅ Quick send completed!")
        print(f"📤 Sent {stats['sent_records']} records")
        print(f"⚡ Rate: {stats['records_per_second']:.2f} records/second")
        print(f"🎯 Success rate: {stats['success_rate']:.1f}%")
        
    except Exception as e:
        print(f"❌ Quick send failed: {str(e)}")


def run_all_examples():
    """Run all examples in sequence"""
    print("🚀 RUNNING ALL DATA INGESTION PIPELINE EXAMPLES")
    print("=" * 80)
    
    # Check if sample data exists
    if not Path("CustomersData.csv").exists():
        print("⚠️ Sample data file 'CustomersData.csv' not found!")
        print("Please ensure the file exists in the current directory.")
        return
    
    try:
        # Run examples that don't require Kafka first
        example_file_validation()
        example_file_processor()
        
        # Note about Kafka examples
        print("\n" + "=" * 60)
        print("KAFKA-DEPENDENT EXAMPLES")
        print("=" * 60)
        print("The following examples require Kafka to be running.")
        print("Please start Kafka using docker-compose in the kafka-docker directory:")
        print("  cd kafka-docker")
        print("  docker-compose up -d")
        print("")
        
        # Uncomment these if Kafka is running
        # example_file_to_kafka()
        # example_kafka_to_analysis() 
        # example_full_pipeline()
        # example_quick_kafka_send()
        
        print("📝 To run Kafka examples, uncomment the lines in run_all_examples()")
        
    except Exception as e:
        print(f"❌ Examples failed: {str(e)}")
    
    print("\n" + "=" * 80)
    print("🎉 EXAMPLES COMPLETED!")
    print("Check the generated output directories for results:")
    print("  • validation_output/")
    print("  • file_processor_output/")
    print("  • reports/")
    print("=" * 80)


if __name__ == "__main__":
    run_all_examples()
