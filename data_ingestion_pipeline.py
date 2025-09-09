"""
Main application orchestrating the complete data ingestion pipeline.
Provides command-line interface and orchestrates all services.
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import Optional, Dict, Any
import json
import pandas as pd

# Add the project root to the path
sys.path.append(str(Path(__file__).parent))

from services.file_processor import FileProcessor
from services.kafka_producer_enhanced import EnhancedKafkaProducer
from services.kafka_consumer_enhanced import EnhancedKafkaConsumer
from services.data_validator import DataValidator
from services.data_reporter import DataQualityReporter

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_ingestion.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class DataIngestionPipeline:
    """Main orchestrator for the data ingestion pipeline"""
    
    def __init__(self, 
                 kafka_servers: list = None,
                 default_topic: str = "data_ingestion_topic"):
        """
        Initialize the data ingestion pipeline
        
        Args:
            kafka_servers: List of Kafka bootstrap servers
            default_topic: Default Kafka topic for data ingestion
        """
        self.kafka_servers = kafka_servers or ["localhost:9092"]
        self.default_topic = default_topic
        
        # Initialize services
        self.file_processor = FileProcessor()
        self.data_validator = DataValidator()
        self.reporter = DataQualityReporter()
        
        logger.info("Data Ingestion Pipeline initialized")
        logger.info(f"Kafka servers: {self.kafka_servers}")
        logger.info(f"Default topic: {self.default_topic}")
    
    def run_file_to_kafka_pipeline(self, 
                                  input_file: str,
                                  topic: str = None,
                                  validate_before_send: bool = True,
                                  chunk_size: int = 1000) -> Dict[str, Any]:
        """
        Run pipeline: File → Validation → Kafka
        
        Args:
            input_file: Path to input data file
            topic: Kafka topic (uses default if None)
            validate_before_send: Whether to validate data before sending
            chunk_size: Chunk size for processing
            
        Returns:
            Dict containing pipeline results
        """
        topic = topic or self.default_topic
        logger.info(f"Starting file-to-Kafka pipeline: {input_file} → {topic}")
        
        results = {
            "pipeline_type": "file_to_kafka",
            "input_file": input_file,
            "topic": topic,
            "validation_enabled": validate_before_send
        }
        
        try:
            # Step 1: Load and process file
            logger.info("Step 1: Loading data from file...")
            df = self.file_processor.load_data_from_file(input_file)
            records = self.file_processor.convert_dataframe_to_records(df, clean_data=True)
            
            results["file_processing"] = {
                "total_records_loaded": len(records),
                "columns": list(df.columns) if not df.empty else []
            }
            
            # Step 2: Validate data (if enabled)
            if validate_before_send:
                logger.info("Step 2: Validating data...")
                validation_results = self.data_validator.validate_batch(records, input_file)
                good_data = self.data_validator.get_good_data()
                
                results["validation"] = {
                    "total_validated": len(validation_results),
                    "valid_records": len(good_data),
                    "invalid_records": len(validation_results) - len(good_data),
                    "success_rate": f"{(len(good_data) / len(validation_results) * 100):.2f}%" if validation_results else "0%"
                }
                
                # Use only valid records for Kafka
                records_to_send = [record.dict() for record in good_data]
            else:
                records_to_send = records
            
            # Step 3: Send to Kafka
            logger.info("Step 3: Sending data to Kafka...")
            producer = EnhancedKafkaProducer(self.kafka_servers)
            
            try:
                kafka_stats = producer.send_batch(records_to_send, topic)
                results["kafka_sending"] = kafka_stats
            finally:
                producer.close()
            
            # Step 4: Generate report (if validation was done)
            if validate_before_send:
                logger.info("Step 4: Generating validation report...")
                stats = self.data_validator.get_statistics()
                report_path = self.reporter.generate_comprehensive_report(
                    self.data_validator.validation_results,
                    stats,
                    output_format="html"
                )
                results["report_path"] = report_path
            
            results["status"] = "success"
            logger.info("File-to-Kafka pipeline completed successfully")
            
        except Exception as e:
            results["status"] = "error"
            results["error"] = str(e)
            logger.error(f"Pipeline failed: {str(e)}")
            raise
        
        return results
    
    def run_kafka_to_analysis_pipeline(self,
                                     topic: str = None,
                                     max_records: int = None,
                                     timeout_seconds: int = 60,
                                     quality_threshold: float = 0.8,
                                     output_dir: str = "output") -> Dict[str, Any]:
        """
        Run pipeline: Kafka → Validation → Analysis → Reports
        
        Args:
            topic: Kafka topic to consume from
            max_records: Maximum records to process
            timeout_seconds: Timeout for consumption
            quality_threshold: Quality threshold for data classification
            output_dir: Output directory for results
            
        Returns:
            Dict containing pipeline results
        """
        topic = topic or self.default_topic
        logger.info(f"Starting Kafka-to-analysis pipeline from topic: {topic}")
        
        try:
            # Step 1: Consume and validate from Kafka
            logger.info("Step 1: Consuming data from Kafka...")
            consumer = EnhancedKafkaConsumer(topic, self.kafka_servers)
            
            results = consumer.start_consuming(
                max_records=max_records,
                timeout_seconds=timeout_seconds,
                quality_threshold=quality_threshold
            )
            
            # Step 2: Save results to files
            logger.info("Step 2: Saving results to files...")
            consumer.save_results_to_files(results, output_dir)
            
            # Step 3: Generate comprehensive report
            logger.info("Step 3: Generating comprehensive report...")
            validation_results = consumer.validator.validation_results
            stats = consumer.validator.get_statistics()
            
            report_path = self.reporter.generate_comprehensive_report(
                validation_results,
                stats,
                output_format="html"
            )
            
            results["report_path"] = report_path
            results["output_directory"] = output_dir
            
            # Step 4: Generate dashboard
            logger.info("Step 4: Generating quality dashboard...")
            dashboard = consumer.create_data_quality_dashboard(results)
            print("\\n" + dashboard)
            
            results["status"] = "success"
            logger.info("Kafka-to-analysis pipeline completed successfully")
            
            consumer.stop_consuming()
            
        except Exception as e:
            results = {
                "status": "error",
                "error": str(e),
                "pipeline_type": "kafka_to_analysis"
            }
            logger.error(f"Pipeline failed: {str(e)}")
            raise
        
        return results
    
    def run_full_pipeline(self,
                         input_file: str,
                         topic: str = None,
                         max_records: int = None,
                         timeout_seconds: int = 60,
                         quality_threshold: float = 0.8,
                         output_dir: str = "output") -> Dict[str, Any]:
        """
        Run complete pipeline: File → Kafka → Validation → Analysis → Reports
        
        Args:
            input_file: Path to input data file
            topic: Kafka topic
            max_records: Maximum records to process
            timeout_seconds: Timeout for consumption
            quality_threshold: Quality threshold
            output_dir: Output directory
            
        Returns:
            Dict containing complete pipeline results
        """
        topic = topic or self.default_topic
        logger.info(f"Starting full pipeline: {input_file} → {topic} → analysis")
        
        results = {
            "pipeline_type": "full_pipeline",
            "input_file": input_file,
            "topic": topic
        }
        
        try:
            # Phase 1: File to Kafka
            logger.info("Phase 1: File to Kafka...")
            file_to_kafka_results = self.run_file_to_kafka_pipeline(
                input_file=input_file,
                topic=topic,
                validate_before_send=True
            )
            results["phase1_file_to_kafka"] = file_to_kafka_results
            
            # Small delay to ensure messages are available
            import time
            time.sleep(2)
            
            # Phase 2: Kafka to Analysis
            logger.info("Phase 2: Kafka to Analysis...")
            kafka_to_analysis_results = self.run_kafka_to_analysis_pipeline(
                topic=topic,
                max_records=max_records,
                timeout_seconds=timeout_seconds,
                quality_threshold=quality_threshold,
                output_dir=output_dir
            )
            results["phase2_kafka_to_analysis"] = kafka_to_analysis_results
            
            results["status"] = "success"
            results["final_report"] = kafka_to_analysis_results.get("report_path")
            results["output_directory"] = output_dir
            
            logger.info("Full pipeline completed successfully")
            
        except Exception as e:
            results["status"] = "error"
            results["error"] = str(e)
            logger.error(f"Full pipeline failed: {str(e)}")
            raise
        
        return results
    
    def validate_file_only(self, 
                          input_file: str,
                          output_dir: str = "output",
                          quality_threshold: float = 0.8) -> Dict[str, Any]:
        """
        Validate a file without using Kafka
        
        Args:
            input_file: Path to input data file
            output_dir: Output directory for results
            quality_threshold: Quality threshold for classification
            
        Returns:
            Dict containing validation results
        """
        logger.info(f"Starting file validation: {input_file}")
        
        try:
            # Load data
            df = self.file_processor.load_data_from_file(input_file)
            records = self.file_processor.convert_dataframe_to_records(df, clean_data=True)
            
            # Validate
            validation_results = self.data_validator.validate_batch(records, input_file)
            
            # Get data splits
            good_data = self.data_validator.get_good_data()
            bad_data = self.data_validator.get_bad_data()
            high_quality_data, low_quality_data = self.data_validator.get_quality_filtered_data(quality_threshold)
            
            # Save results
            Path(output_dir).mkdir(parents=True, exist_ok=True)
            
            if good_data:
                good_df = pd.DataFrame(good_data)
                self.file_processor.save_dataframe(good_df, f"{output_dir}/good_data.csv")
            
            if high_quality_data:
                hq_df = pd.DataFrame(high_quality_data)
                self.file_processor.save_dataframe(hq_df, f"{output_dir}/high_quality_data.csv")
            
            # Generate report
            stats = self.data_validator.get_statistics()
            report_path = self.reporter.generate_comprehensive_report(
                validation_results,
                stats,
                output_format="html"
            )
            
            results = {
                "status": "success",
                "total_records": len(records),
                "valid_records": len(good_data),
                "invalid_records": len(bad_data),
                "high_quality_records": len(high_quality_data),
                "low_quality_records": len(low_quality_data),
                "success_rate": f"{(len(good_data) / len(records) * 100):.2f}%" if records else "0%",
                "report_path": report_path,
                "output_directory": output_dir
            }
            
            logger.info(f"File validation completed: {results['success_rate']} success rate")
            return results
            
        except Exception as e:
            logger.error(f"File validation failed: {str(e)}")
            raise


def main():
    """Command-line interface for the data ingestion pipeline"""
    parser = argparse.ArgumentParser(description="Enhanced Data Ingestion Pipeline")
    parser.add_argument("command", choices=["file-to-kafka", "kafka-to-analysis", "full-pipeline", "validate-file"],
                      help="Pipeline command to execute")
    parser.add_argument("--input-file", "-i", help="Input data file path")
    parser.add_argument("--topic", "-t", default="data_ingestion_topic", help="Kafka topic")
    parser.add_argument("--kafka-servers", "-k", nargs="+", default=["localhost:9092"], 
                      help="Kafka bootstrap servers")
    parser.add_argument("--max-records", "-m", type=int, help="Maximum records to process")
    parser.add_argument("--timeout", type=int, default=60, help="Timeout in seconds")
    parser.add_argument("--quality-threshold", "-q", type=float, default=0.8, 
                      help="Quality threshold for data classification")
    parser.add_argument("--output-dir", "-o", default="output", help="Output directory")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Initialize pipeline
    pipeline = DataIngestionPipeline(
        kafka_servers=args.kafka_servers,
        default_topic=args.topic
    )
    
    try:
        if args.command == "file-to-kafka":
            if not args.input_file:
                print("Error: --input-file is required for file-to-kafka command")
                sys.exit(1)
            
            results = pipeline.run_file_to_kafka_pipeline(
                input_file=args.input_file,
                topic=args.topic
            )
            
        elif args.command == "kafka-to-analysis":
            results = pipeline.run_kafka_to_analysis_pipeline(
                topic=args.topic,
                max_records=args.max_records,
                timeout_seconds=args.timeout,
                quality_threshold=args.quality_threshold,
                output_dir=args.output_dir
            )
            
        elif args.command == "full-pipeline":
            if not args.input_file:
                print("Error: --input-file is required for full-pipeline command")
                sys.exit(1)
            
            results = pipeline.run_full_pipeline(
                input_file=args.input_file,
                topic=args.topic,
                max_records=args.max_records,
                timeout_seconds=args.timeout,
                quality_threshold=args.quality_threshold,
                output_dir=args.output_dir
            )
            
        elif args.command == "validate-file":
            if not args.input_file:
                print("Error: --input-file is required for validate-file command")
                sys.exit(1)
            
            results = pipeline.validate_file_only(
                input_file=args.input_file,
                output_dir=args.output_dir,
                quality_threshold=args.quality_threshold
            )
        
        # Print results summary
        print("\\n" + "="*60)
        print("PIPELINE EXECUTION SUMMARY")
        print("="*60)
        print(f"Command: {args.command}")
        print(f"Status: {results.get('status', 'unknown')}")
        
        if 'error' in results:
            print(f"Error: {results['error']}")
        else:
            print("Execution completed successfully!")
            if 'report_path' in results:
                print(f"Report generated: {results['report_path']}")
            if 'output_directory' in results:
                print(f"Output directory: {results['output_directory']}")
        
        print("="*60)
        
        # Save full results
        results_file = Path(args.output_dir) / "pipeline_results.json"
        results_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(results_file, 'w') as f:
            # Remove non-serializable objects
            serializable_results = {k: v for k, v in results.items() 
                                  if not k.startswith('dataframes') and k != 'raw_results'}
            json.dump(serializable_results, f, indent=2, default=str)
        
        print(f"Full results saved to: {results_file}")
        
    except Exception as e:
        logger.error(f"Pipeline execution failed: {str(e)}")
        print(f"\\nError: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
