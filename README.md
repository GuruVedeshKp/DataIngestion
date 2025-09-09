# 🚀 Enhanced Data Ingestion Pipeline

**An intelligent, schema-agnostic data ingestion and validation system powered by Kafka, Pandas, and advanced machine learning techniques.**

## 🌟 Overview

This pipeline revolutionizes data processing by automatically understanding any data structure and providing intelligent validation, quality assessment, and comprehensive reporting. No more manual schema definitions—the system learns and adapts to your data!

## ✨ Key Features

### 🧠 **Intelligent Schema Inference**
- **Zero Configuration**: Automatically understands any data structure
- **15+ Data Type Detection**: From basic types to complex patterns (emails, phones, currencies, etc.)
- **Quality Scoring**: Advanced quality metrics for every field and record
- **Adaptive Learning**: Continuously improves understanding of your data

### 🔄 **Complete Data Pipeline**
- **Multi-Format Support**: CSV, Excel, JSON, Parquet, and more
- **Kafka Integration**: Real-time streaming with enhanced producers/consumers
- **Validation & Quality Control**: Automatic good/bad data separation
- **Comprehensive Reporting**: HTML, Markdown, and text reports with insights

### 📊 **Advanced Analytics**
- **Quality Dashboards**: Visual insights into data quality
- **Pattern Recognition**: Detects anomalies and inconsistencies
- **Data Profiling**: Automatic statistical analysis and summaries
- **Performance Metrics**: Processing throughput and efficiency tracking
- **Pipeline Orchestration**: Complete pipeline orchestration with CLI interface
- **Multiple Output Formats**: Save results in CSV, JSON, Excel, Parquet

## 📁 Project Structure

```
DataIngestion/
├── models/
│   ├── __init__.py
│   └── data_models.py              # Pydantic models and validation schemas
├── services/
│   ├── __init__.py
│   ├── file_processor.py           # Multi-format file processing
│   ├── kafka_producer_enhanced.py  # Enhanced Kafka producer
│   ├── kafka_consumer_enhanced.py  # Enhanced Kafka consumer
│   ├── data_validator.py           # Data validation service
│   └── data_reporter.py            # Quality reporting service
├── kafka-docker/
│   └── docker-compose.yml          # Kafka Docker setup
├── data_ingestion_pipeline.py      # Main pipeline orchestrator
├── examples.py                     # Usage examples
├── requirements.txt                # Python dependencies
└── README.md                       # This file
```

## 🛠️ Installation

### Prerequisites
- Python 3.8+
- Docker and Docker Compose (for Kafka)

### Install Dependencies

```bash
pip install -r requirements.txt
```

### Start Kafka (Required for Kafka-based pipelines)

```bash
cd kafka-docker
docker-compose up -d
```

## 🎯 Quick Start

### 1. Validate Data File Only

```bash
python data_ingestion_pipeline.py validate-file --input-file CustomersData.csv
```

### 2. Send File to Kafka with Validation

```bash
python data_ingestion_pipeline.py file-to-kafka --input-file CustomersData.csv --topic customer_data
```

### 3. Consume from Kafka and Analyze

```bash
python data_ingestion_pipeline.py kafka-to-analysis --topic customer_data --timeout 30
```

### 4. Full Pipeline (File → Kafka → Analysis)

```bash
python data_ingestion_pipeline.py full-pipeline --input-file CustomersData.csv --topic customer_data
```

### 5. Run Examples

```bash
python examples.py
```

## 📚 Detailed Usage

### Command Line Interface

The pipeline provides a comprehensive CLI with the following commands:

#### Commands
- `validate-file`: Validate file without Kafka
- `file-to-kafka`: Send file data to Kafka
- `kafka-to-analysis`: Consume and analyze Kafka data
- `full-pipeline`: Complete end-to-end pipeline

#### Common Options
- `--input-file, -i`: Input data file path
- `--topic, -t`: Kafka topic (default: data_ingestion_topic)
- `--kafka-servers, -k`: Kafka bootstrap servers (default: localhost:9092)
- `--max-records, -m`: Maximum records to process
- `--timeout`: Timeout in seconds (default: 60)
- `--quality-threshold, -q`: Quality threshold (default: 0.8)
- `--output-dir, -o`: Output directory (default: output)
- `--verbose, -v`: Enable verbose logging

### Python API Usage

#### File Validation Only

```python
from data_ingestion_pipeline import DataIngestionPipeline

pipeline = DataIngestionPipeline()
results = pipeline.validate_file_only(
    input_file="data.csv",
    quality_threshold=0.8
)
print(f"Success rate: {results['success_rate']}")
```

#### Using Individual Services

```python
from services.file_processor import FileProcessor
from services.data_validator import DataValidator

# Process file
processor = FileProcessor()
df = processor.load_data_from_file("data.xlsx")  # Auto-detects format
records = processor.convert_dataframe_to_records(df)

# Validate data
validator = DataValidator()
results = validator.validate_batch(records)
good_data = validator.get_good_data()
bad_data = validator.get_bad_data()
```

#### Kafka Operations

```python
from services.kafka_producer_enhanced import send_csv_to_kafka
from services.kafka_consumer_enhanced import EnhancedKafkaConsumer

# Quick send to Kafka
stats = send_csv_to_kafka("data.csv", "my_topic")

# Enhanced consumption
consumer = EnhancedKafkaConsumer("my_topic")
results = consumer.start_consuming(max_records=1000, timeout_seconds=60)
```

## 📊 Data Model and Validation

### Customer Transaction Model

The pipeline uses a comprehensive Pydantic model for customer transactions:

```python
class CustomerTransaction(BaseModel):
    customer_id: str
    company_name: str
    contact_name: str
    contact_title: str
    city: str
    region: Optional[str]
    postal_code: str
    country: str
    segment: str
    metro_area: Union[bool, str]
    # ... additional fields
```

### Validation Features

- **Field Validation**: Type checking, length validation, format validation
- **Data Cleaning**: Automatic trimming, case normalization
- **Quality Scoring**: 0-1 quality score based on completeness and validity
- **Error Categorization**: Detailed error messages with field-level information
- **Warning System**: Warnings for data quality issues

### Quality Levels

- **Excellent** (≥90%): Production-ready data
- **Good** (≥80%): Acceptable with minor issues
- **Fair** (≥60%): Requires review
- **Poor** (<60%): Needs significant attention

## 📈 Reporting and Analytics

### Report Types

1. **HTML Reports**: Interactive reports with tables and metrics
2. **Markdown Reports**: Documentation-friendly format
3. **JSON Reports**: Machine-readable format for integration
4. **Dashboard**: Real-time text-based dashboard

### Report Contents

- **Executive Summary**: High-level metrics and KPIs
- **Quality Distribution**: Breakdown by quality levels
- **Error Analysis**: Common error patterns and frequencies
- **Data Insights**: Automated insights and recommendations
- **Processing Statistics**: Performance and throughput metrics

### Sample Report Structure

```
📊 PROCESSING SUMMARY
- Total Records: 1,000
- Valid Records: 950
- Success Rate: 95.0%
- Processing Duration: 2.5s

🎯 QUALITY DISTRIBUTION
- Excellent: 800 (84.2%)
- Good: 120 (12.6%)
- Fair: 25 (2.6%)
- Poor: 5 (0.5%)

💡 INSIGHTS
- Excellent data quality with very high success rate
- Most common issue: postal_code validation
```

## 🔧 Configuration

### File Format Support

The pipeline automatically detects and processes:

- **CSV/TSV**: With encoding detection and delimiter auto-detection
- **JSON**: Standard JSON and JSON Lines formats
- **Excel**: .xlsx and .xls files with sheet selection
- **Parquet**: High-performance columnar format
- **XML**: With configurable parsing options

### Kafka Configuration

Default Kafka settings can be customized:

```python
# Producer settings
producer = EnhancedKafkaProducer(
    bootstrap_servers=["localhost:9092"],
    batch_size=100,
    linger_ms=10
)

# Consumer settings
consumer = EnhancedKafkaConsumer(
    topic="my_topic",
    consumer_group="my_group",
    batch_size=100
)
```

### Quality Thresholds

Adjust quality thresholds for your use case:

```python
# High-quality threshold
high_quality, low_quality = validator.get_quality_filtered_data(
    min_quality_score=0.9
)

# Lenient threshold
high_quality, low_quality = validator.get_quality_filtered_data(
    min_quality_score=0.6
)
```

## 🚦 Error Handling

### Graceful Degradation

- **File Reading**: Multiple encoding fallbacks
- **Kafka Connectivity**: Timeout and retry mechanisms
- **Validation Errors**: Continue processing with error collection
- **Partial Failures**: Process what's possible, report what failed

### Error Categories

1. **File Errors**: File not found, format issues, encoding problems
2. **Validation Errors**: Schema violations, data type mismatches
3. **Kafka Errors**: Connection issues, serialization problems
4. **Processing Errors**: Memory issues, timeout errors

## 📏 Performance Considerations

### Optimization Features

- **Batch Processing**: Configurable batch sizes
- **Memory Management**: Streaming processing for large files
- **Parallel Processing**: Multi-threaded validation
- **Efficient Formats**: Parquet support for large datasets

### Performance Metrics

The pipeline tracks:
- Records processed per second
- Memory usage patterns
- Error rates and patterns
- Processing duration by stage

## 🧪 Testing

### Run Examples

```bash
python examples.py
```

### Manual Testing

1. **File Validation**:
   ```bash
   python data_ingestion_pipeline.py validate-file -i CustomersData.csv
   ```

2. **Performance Test**:
   ```bash
   python data_ingestion_pipeline.py full-pipeline -i CustomersData.csv --verbose
   ```

## 🐛 Troubleshooting

### Common Issues

1. **Kafka Connection Failed**
   - Ensure Kafka is running: `docker-compose up -d`
   - Check ports: `localhost:9092`

2. **File Encoding Issues**
   - Pipeline auto-detects encoding
   - Check file with: `file -bi filename`

3. **Memory Issues with Large Files**
   - Reduce batch size: `--batch-size 50`
   - Use streaming mode for very large files

4. **Permission Errors**
   - Ensure write permissions for output directory
   - Check file access permissions

### Debug Mode

Enable verbose logging:

```bash
python data_ingestion_pipeline.py --verbose full-pipeline -i data.csv
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License.

## 🙏 Acknowledgments

- Pydantic for robust data validation
- Kafka for reliable message streaming
- Pandas for efficient data processing
- The open-source community for inspiration and tools
