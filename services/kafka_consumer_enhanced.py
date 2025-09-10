"""
Enhanced Kafka consumer with data validation, quality assessment, and reporting.
Integrates with Pydantic models and data validation services.
"""

from confluent_kafka import Consumer, KafkaException, KafkaError
import json
import pandas as pd
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
import time

from models.data_models import CustomerTransaction, DataValidationResult, DataIngestionStats
from services.data_validator import DataValidator
from services.file_processor import FileProcessor

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class EnhancedKafkaConsumer:
    """Enhanced Kafka consumer with data validation and quality assessment"""
    
    def __init__(self, 
                 topic: str,
                 bootstrap_servers: List[str] = None,
                 consumer_group: str = "data_ingestion_group",
                 auto_offset_reset: str = "earliest",
                 enable_auto_commit: bool = True,
                 batch_size: int = 100,
                 max_poll_records: int = 500):
        """
        Initialize enhanced Kafka consumer
        
        Args:
            topic: Kafka topic to consume from
            bootstrap_servers: List of Kafka bootstrap servers
            consumer_group: Consumer group ID
            auto_offset_reset: Auto offset reset policy
            enable_auto_commit: Whether to enable auto commit
            batch_size: Batch size for processing
            max_poll_records: Maximum records per poll
        """
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers or ["localhost:9092"]
        self.batch_size = batch_size
        
        # Initialize services
        self.validator = DataValidator()
        self.file_processor = FileProcessor()
        
        # Configure consumer settings for confluent-kafka
        consumer_config = {
            'bootstrap.servers': ','.join(self.bootstrap_servers),
            'group.id': consumer_group,
            'auto.offset.reset': auto_offset_reset,
            'enable.auto.commit': enable_auto_commit,
            'max.poll.interval.ms': 300000,
            'session.timeout.ms': 10000,
            'heartbeat.interval.ms': 3000
        }
        
        logger.info(f"Consumer config: group.id={consumer_group}, auto.offset.reset={auto_offset_reset}")
        
        # Initialize Kafka consumer
        self.consumer = Consumer(consumer_config)
        self.consumer.subscribe([topic])
        
        logger.info(f"Subscribed to topic: {topic}")
        
        # Processing state
        self.processed_records = []
        self.is_running = False
        self.processing_start_time = None
        
        logger.info(f"Enhanced Kafka Consumer initialized for topic: {topic}")
        logger.info(f"Bootstrap servers: {self.bootstrap_servers}")
    
    def start_consuming(self, 
                       max_records: Optional[int] = None,
                       timeout_seconds: Optional[int] = None,
                       quality_threshold: float = 0.8) -> Dict[str, Any]:
        """
        Start consuming messages from Kafka with validation and quality assessment
        
        Args:
            max_records: Maximum number of records to process (None for unlimited)
            timeout_seconds: Timeout in seconds (None for unlimited)
            quality_threshold: Quality threshold for separating good/bad data
            
        Returns:
            Dict containing processing results and statistics
        """
        logger.info("Starting enhanced Kafka consumer...")
        logger.info(f"Max records: {max_records}, Timeout: {timeout_seconds}s, Quality threshold: {quality_threshold}")
        
        self.is_running = True
        self.processing_start_time = datetime.now()
        self.validator.reset_stats()
        
        processed_count = 0
        batch_buffer = []
        consecutive_empty_polls = 0
        max_empty_polls = 15  # Reduce to 15 seconds for faster detection
        
        try:
            start_time = time.time()
            logger.info(f"Starting message consumption. Will stop after {max_empty_polls} consecutive empty polls or {timeout_seconds}s timeout.")
            
            while self.is_running:
                # Poll for messages
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    consecutive_empty_polls += 1
                    
                    # Log progress every 5 empty polls
                    if consecutive_empty_polls % 5 == 0:
                        logger.info(f"No messages received. Empty polls: {consecutive_empty_polls}/{max_empty_polls}. Processed so far: {processed_count}")
                    
                    # If we've processed some records and now getting empty polls, consider stopping
                    if consecutive_empty_polls >= max_empty_polls:
                        if processed_count > 0:
                            logger.info(f"No new messages after {max_empty_polls} polls. Processed {processed_count} records. Finishing consumption.")
                            break
                        else:
                            logger.warning(f"No messages found in topic after {max_empty_polls} polls. Topic may be empty.")
                            break
                    continue
                else:
                    consecutive_empty_polls = 0  # Reset counter when we get a message
                    
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.info('End of partition reached')
                        consecutive_empty_polls += 1
                        if consecutive_empty_polls >= 5:  # Stop faster on EOF
                            logger.info("End of partition reached and no more messages. Finishing consumption.")
                            break
                        continue
                    else:
                        logger.error(f'Consumer error: {msg.error()}')
                        break
                
                # Check timeout
                if timeout_seconds and (time.time() - start_time) > timeout_seconds:
                    logger.info(f"Timeout reached ({timeout_seconds}s). Processed {processed_count} records.")
                    break
                
                # Deserialize message
                try:
                    record = json.loads(msg.value().decode('utf-8'))
                    batch_buffer.append(record)
                    processed_count += 1
                    
                    # Process batch when buffer is full
                    if len(batch_buffer) >= self.batch_size:
                        self._process_batch(batch_buffer)
                        batch_buffer = []
                    
                    # Check max records limit
                    if max_records and processed_count >= max_records:
                        logger.info(f"Max records limit reached ({max_records})")
                        break
                        
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to decode message: {e}")
                    continue
                
                # Log progress
                if processed_count % 50 == 0:
                    logger.info(f"Processed {processed_count} messages...")
            
            # Process remaining records in buffer
            if batch_buffer:
                self._process_batch(batch_buffer)
            
        except KeyboardInterrupt:
            logger.info("Consumer interrupted by user")
        except Exception as e:
            logger.error(f"Error during consumption: {str(e)}")
            raise
        finally:
            self.is_running = False
        
        # Generate final results
        results = self._generate_results(quality_threshold)
        logger.info(f"Consumer finished. Total processed: {processed_count}")
        
        return results
    
    def _process_batch(self, batch: List[Dict[str, Any]]):
        """Process a batch of records with validation"""
        logger.debug(f"Processing batch of {len(batch)} records")
        
        try:
            # Validate batch
            validation_results = self.validator.validate_batch(batch, source_file="kafka_stream")
            
            # Store results
            self.processed_records.extend(validation_results)
            
            # Log batch statistics
            valid_count = sum(1 for r in validation_results if r.is_valid)
            invalid_count = len(validation_results) - valid_count
            
            logger.debug(f"Batch processed: {valid_count} valid, {invalid_count} invalid")
            
        except Exception as e:
            logger.error(f"Error processing batch: {str(e)}")
            # Continue processing even if batch fails
    
    def stop_consuming(self):
        """Stop the consumer gracefully"""
        logger.info("Stopping consumer...")
        self.is_running = False
        if self.consumer:
            self.consumer.close()
    
    def _generate_results(self, quality_threshold: float) -> Dict[str, Any]:
        """Generate comprehensive results and statistics"""
        logger.info("Generating processing results...")
        
        # Get data splits
        good_data = self.validator.get_good_data()
        bad_data = self.validator.get_bad_data()
        high_quality_data, low_quality_data = self.validator.get_quality_filtered_data(quality_threshold)
        
        # Get statistics
        stats = self.validator.get_statistics()
        validation_report = self.validator.export_validation_report()
        
        # Create DataFrames for analysis
        good_df = None
        if good_data:
            good_records = [record.dict() for record in good_data]
            good_df = pd.DataFrame(good_records)
        
        high_quality_df = None
        if high_quality_data:
            high_quality_records = [record.dict() for record in high_quality_data]
            high_quality_df = pd.DataFrame(high_quality_records)
        
        results = {
            "processing_summary": {
                "total_records": stats.total_records,
                "valid_records": stats.valid_records,
                "invalid_records": stats.invalid_records,
                "success_rate": f"{stats.success_rate:.2f}%",
                "processing_duration": f"{stats.processing_duration:.2f}s" if stats.processing_duration else "N/A",
                "records_per_second": stats.total_records / stats.processing_duration if stats.processing_duration else 0
            },
            "quality_metrics": {
                "excellent_quality": stats.excellent_quality,
                "good_quality": stats.good_quality,
                "fair_quality": stats.fair_quality,
                "poor_quality": stats.poor_quality,
                "high_quality_count": len(high_quality_data),
                "low_quality_count": len(low_quality_data)
            },
            "data_splits": {
                "good_data_count": len(good_data),
                "bad_data_count": len(bad_data),
                "quality_threshold": quality_threshold
            },
            "dataframes": {
                "all_good_data": good_df,
                "high_quality_data": high_quality_df
            },
            "validation_report": validation_report,
            "raw_results": {
                "good_data": good_data,
                "bad_data": bad_data,
                "high_quality_data": high_quality_data,
                "low_quality_data": low_quality_data
            }
        }
        
        return results
    
    def save_results_to_files(self, 
                             results: Dict[str, Any],
                             output_dir: str = "output",
                             file_format: str = "csv"):
        """
        Save processing results to files
        
        Args:
            results: Results dictionary from start_consuming()
            output_dir: Output directory path
            file_format: Output file format (csv, json, excel, parquet)
        """
        logger.info(f"Saving results to {output_dir} in {file_format} format")
        
        from pathlib import Path
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        try:
            # Save good data
            good_df = results["dataframes"]["all_good_data"]
            if good_df is not None and not good_df.empty:
                good_file = output_path / f"good_data_{timestamp}.{file_format}"
                self.file_processor.save_dataframe(good_df, str(good_file), file_format)
                logger.info(f"Saved good data to {good_file}")
            
            # Save high quality data
            high_quality_df = results["dataframes"]["high_quality_data"]
            if high_quality_df is not None and not high_quality_df.empty:
                hq_file = output_path / f"high_quality_data_{timestamp}.{file_format}"
                self.file_processor.save_dataframe(high_quality_df, str(hq_file), file_format)
                logger.info(f"Saved high quality data to {hq_file}")
            
            # Save validation report
            report_file = output_path / f"validation_report_{timestamp}.json"
            with open(report_file, 'w') as f:
                json.dump(results["validation_report"], f, indent=2, default=str)
            logger.info(f"Saved validation report to {report_file}")
            
            # Save processing summary
            summary_file = output_path / f"processing_summary_{timestamp}.json"
            summary_data = {
                "processing_summary": results["processing_summary"],
                "quality_metrics": results["quality_metrics"],
                "data_splits": results["data_splits"]
            }
            with open(summary_file, 'w') as f:
                json.dump(summary_data, f, indent=2, default=str)
            logger.info(f"Saved processing summary to {summary_file}")
            
        except Exception as e:
            logger.error(f"Error saving results: {str(e)}")
            raise
    
    def get_realtime_stats(self) -> Dict[str, Any]:
        """Get real-time processing statistics"""
        stats = self.validator.get_statistics()
        
        return {
            "total_processed": stats.total_records,
            "valid_records": stats.valid_records,
            "invalid_records": stats.invalid_records,
            "success_rate": f"{stats.success_rate:.2f}%",
            "is_running": self.is_running,
            "processing_duration": stats.processing_duration or 0
        }
    
    def create_data_quality_dashboard(self, results: Dict[str, Any]) -> str:
        """Create a simple text-based data quality dashboard"""
        dashboard = []
        dashboard.append("=" * 60)
        dashboard.append("DATA INGESTION QUALITY DASHBOARD")
        dashboard.append("=" * 60)
        dashboard.append("")
        
        # Processing Summary
        summary = results["processing_summary"]
        dashboard.append("📊 PROCESSING SUMMARY")
        dashboard.append("-" * 30)
        dashboard.append(f"Total Records Processed: {summary['total_records']:,}")
        dashboard.append(f"Valid Records: {summary['valid_records']:,}")
        dashboard.append(f"Invalid Records: {summary['invalid_records']:,}")
        dashboard.append(f"Success Rate: {summary['success_rate']}")
        dashboard.append(f"Processing Duration: {summary['processing_duration']}")
        dashboard.append(f"Records/Second: {summary['records_per_second']:.2f}")
        dashboard.append("")
        
        # Quality Distribution
        quality = results["quality_metrics"]
        dashboard.append("🎯 QUALITY DISTRIBUTION")
        dashboard.append("-" * 30)
        dashboard.append(f"Excellent Quality: {quality['excellent_quality']:,}")
        dashboard.append(f"Good Quality: {quality['good_quality']:,}")
        dashboard.append(f"Fair Quality: {quality['fair_quality']:,}")
        dashboard.append(f"Poor Quality: {quality['poor_quality']:,}")
        dashboard.append("")
        
        # Data Quality Insights
        insights = results["validation_report"]["data_quality_insights"]
        dashboard.append("💡 QUALITY INSIGHTS")
        dashboard.append("-" * 30)
        for insight in insights:
            dashboard.append(f"• {insight}")
        dashboard.append("")
        
        dashboard.append("=" * 60)
        
        return "\n".join(dashboard)
