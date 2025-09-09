"""
Enhanced Kafka producer that can read data from multiple file formats
and send to Kafka with proper error handling and monitoring.
"""

from kafka import KafkaProducer
import json
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
import time

from services.file_processor import FileProcessor

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class EnhancedKafkaProducer:
    """Enhanced Kafka producer with file format support and monitoring"""
    
    def __init__(self, 
                 bootstrap_servers: List[str] = None,
                 batch_size: int = 100,
                 linger_ms: int = 10,
                 buffer_memory: int = 33554432,  # 32MB
                 max_request_size: int = 1048576):  # 1MB
        """
        Initialize enhanced Kafka producer
        
        Args:
            bootstrap_servers: List of Kafka bootstrap servers
            batch_size: Batch size for sending
            linger_ms: Linger time in milliseconds
            buffer_memory: Buffer memory size
            max_request_size: Maximum request size
        """
        self.bootstrap_servers = bootstrap_servers or ["localhost:9092"]
        self.batch_size = batch_size
        
        # Initialize file processor
        self.file_processor = FileProcessor()
        
        # Initialize Kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            batch_size=batch_size,
            linger_ms=linger_ms,
            buffer_memory=buffer_memory,
            max_request_size=max_request_size,
            value_serializer=lambda v: json.dumps(v, default=self._json_serializer).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None
        )
        
        # Statistics
        self.sent_count = 0
        self.error_count = 0
        self.start_time = None
        
        logger.info("Enhanced Kafka Producer initialized")
        logger.info(f"Bootstrap servers: {self.bootstrap_servers}")
    
    def _json_serializer(self, obj):
        """Custom JSON serializer for handling special types"""
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif hasattr(obj, '__dict__'):
            return obj.__dict__
        else:
            return str(obj)
    
    def send_from_file(self, 
                      file_path: str, 
                      topic: str,
                      key_field: Optional[str] = None,
                      chunk_size: int = 1000,
                      delay_between_chunks: float = 0.1) -> Dict[str, Any]:
        """
        Send data from file to Kafka topic
        
        Args:
            file_path: Path to the data file
            topic: Kafka topic to send to
            key_field: Field to use as message key (optional)
            chunk_size: Number of records to send in each chunk
            delay_between_chunks: Delay between chunks in seconds
            
        Returns:
            Dict containing sending statistics
        """
        logger.info(f"Starting to send data from {file_path} to topic {topic}")
        
        self.start_time = time.time()
        self.sent_count = 0
        self.error_count = 0
        
        try:
            # Load data from file
            logger.info("Loading data from file...")
            df = self.file_processor.load_data_from_file(file_path)
            records = self.file_processor.convert_dataframe_to_records(df, clean_data=True)
            
            total_records = len(records)
            logger.info(f"Loaded {total_records:,} records from file")
            
            # Send data in chunks
            for i in range(0, total_records, chunk_size):
                chunk = records[i:i + chunk_size]
                self._send_chunk(chunk, topic, key_field)
                
                # Log progress
                processed = min(i + chunk_size, total_records)
                progress = (processed / total_records) * 100
                logger.info(f"Progress: {processed:,}/{total_records:,} ({progress:.1f}%)")
                
                # Delay between chunks to avoid overwhelming Kafka
                if delay_between_chunks > 0 and i + chunk_size < total_records:
                    time.sleep(delay_between_chunks)
            
            # Flush remaining messages
            logger.info("Flushing remaining messages...")
            self.producer.flush()
            
            # Calculate statistics
            end_time = time.time()
            duration = end_time - self.start_time
            records_per_second = self.sent_count / duration if duration > 0 else 0
            
            stats = {
                "total_records": total_records,
                "sent_records": self.sent_count,
                "error_records": self.error_count,
                "success_rate": (self.sent_count / total_records) * 100 if total_records > 0 else 0,
                "duration_seconds": duration,
                "records_per_second": records_per_second,
                "source_file": file_path,
                "target_topic": topic
            }
            
            logger.info(f"Data sending completed:")
            logger.info(f"  Total records: {stats['total_records']:,}")
            logger.info(f"  Sent successfully: {stats['sent_records']:,}")
            logger.info(f"  Errors: {stats['error_records']:,}")
            logger.info(f"  Success rate: {stats['success_rate']:.2f}%")
            logger.info(f"  Duration: {stats['duration_seconds']:.2f}s")
            logger.info(f"  Records/second: {stats['records_per_second']:.2f}")
            
            return stats
            
        except Exception as e:
            logger.error(f"Error sending data from file: {str(e)}")
            raise
    
    def _send_chunk(self, chunk: List[Dict[str, Any]], topic: str, key_field: Optional[str]):
        """Send a chunk of records to Kafka"""
        for record in chunk:
            try:
                # Extract key if specified
                key = None
                if key_field and key_field in record:
                    key = str(record[key_field])
                
                # Send message
                future = self.producer.send(topic, value=record, key=key)
                
                # Add callback for monitoring
                future.add_callback(self._on_send_success)
                future.add_errback(self._on_send_error)
                
            except Exception as e:
                logger.error(f"Error sending record: {str(e)}")
                self.error_count += 1
    
    def _on_send_success(self, record_metadata):
        """Callback for successful sends"""
        self.sent_count += 1
        logger.debug(f"Message sent successfully to {record_metadata.topic}:{record_metadata.partition}:{record_metadata.offset}")
    
    def _on_send_error(self, exception):
        """Callback for send errors"""
        self.error_count += 1
        logger.error(f"Failed to send message: {str(exception)}")
    
    def send_batch(self, 
                   records: List[Dict[str, Any]], 
                   topic: str,
                   key_field: Optional[str] = None) -> Dict[str, Any]:
        """
        Send a batch of records to Kafka
        
        Args:
            records: List of record dictionaries
            topic: Kafka topic to send to
            key_field: Field to use as message key (optional)
            
        Returns:
            Dict containing sending statistics
        """
        logger.info(f"Sending batch of {len(records)} records to topic {topic}")
        
        self.start_time = time.time()
        self.sent_count = 0
        self.error_count = 0
        
        try:
            self._send_chunk(records, topic, key_field)
            self.producer.flush()
            
            end_time = time.time()
            duration = end_time - self.start_time
            
            stats = {
                "total_records": len(records),
                "sent_records": self.sent_count,
                "error_records": self.error_count,
                "success_rate": (self.sent_count / len(records)) * 100 if len(records) > 0 else 0,
                "duration_seconds": duration,
                "records_per_second": self.sent_count / duration if duration > 0 else 0,
                "target_topic": topic
            }
            
            logger.info(f"Batch sending completed: {stats['sent_records']}/{stats['total_records']} records sent")
            return stats
            
        except Exception as e:
            logger.error(f"Error sending batch: {str(e)}")
            raise
    
    def send_single_record(self, 
                          record: Dict[str, Any], 
                          topic: str,
                          key: Optional[str] = None) -> bool:
        """
        Send a single record to Kafka
        
        Args:
            record: Record dictionary
            topic: Kafka topic to send to
            key: Message key (optional)
            
        Returns:
            bool: True if sent successfully, False otherwise
        """
        try:
            future = self.producer.send(topic, value=record, key=key)
            record_metadata = future.get(timeout=10)  # Wait up to 10 seconds
            
            logger.debug(f"Record sent to {record_metadata.topic}:{record_metadata.partition}:{record_metadata.offset}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send single record: {str(e)}")
            return False
    
    def close(self):
        """Close the producer and flush any remaining messages"""
        logger.info("Closing Kafka producer...")
        
        try:
            self.producer.flush()
            self.producer.close()
            logger.info("Kafka producer closed successfully")
        except Exception as e:
            logger.error(f"Error closing producer: {str(e)}")
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get current sending statistics"""
        duration = time.time() - self.start_time if self.start_time else 0
        
        return {
            "sent_count": self.sent_count,
            "error_count": self.error_count,
            "duration_seconds": duration,
            "records_per_second": self.sent_count / duration if duration > 0 else 0
        }


# Convenience functions for common use cases
def send_csv_to_kafka(csv_file: str, topic: str, bootstrap_servers: List[str] = None) -> Dict[str, Any]:
    """
    Convenience function to send CSV data to Kafka
    
    Args:
        csv_file: Path to CSV file
        topic: Kafka topic
        bootstrap_servers: Kafka servers (optional)
        
    Returns:
        Dict containing sending statistics
    """
    producer = EnhancedKafkaProducer(bootstrap_servers)
    try:
        return producer.send_from_file(csv_file, topic)
    finally:
        producer.close()


def send_json_to_kafka(json_file: str, topic: str, bootstrap_servers: List[str] = None) -> Dict[str, Any]:
    """
    Convenience function to send JSON data to Kafka
    
    Args:
        json_file: Path to JSON file
        topic: Kafka topic
        bootstrap_servers: Kafka servers (optional)
        
    Returns:
        Dict containing sending statistics
    """
    producer = EnhancedKafkaProducer(bootstrap_servers)
    try:
        return producer.send_from_file(json_file, topic)
    finally:
        producer.close()


def send_excel_to_kafka(excel_file: str, topic: str, bootstrap_servers: List[str] = None) -> Dict[str, Any]:
    """
    Convenience function to send Excel data to Kafka
    
    Args:
        excel_file: Path to Excel file
        topic: Kafka topic
        bootstrap_servers: Kafka servers (optional)
        
    Returns:
        Dict containing sending statistics
    """
    producer = EnhancedKafkaProducer(bootstrap_servers)
    try:
        return producer.send_from_file(excel_file, topic)
    finally:
        producer.close()
