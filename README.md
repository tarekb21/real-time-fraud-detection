# Real-Time Fraud Detection System

A comprehensive real-time fraud detection pipeline built with Apache Kafka, Apache Spark, and Docker. This project demonstrates modern data engineering practices for streaming data processing and machine learning integration.

## Project Overview

This system processes financial transactions in real-time, applying rule-based algorithms to detect fraudulent activities. The architecture follows industry-standard practices for streaming data pipelines and includes a framework for future machine learning model integration.

## Architecture

The system consists of several interconnected components:

- **Data Generation**: Python-based transaction generator with realistic financial data
- **Message Streaming**: Apache Kafka for reliable message queuing and data streaming
- **Stream Processing**: Apache Spark for real-time data processing and fraud detection
- **Data Storage**: Multiple output formats (Parquet, CSV, JSON) for different use cases
- **Visualization**: Interactive web dashboards and Python-based analytics
- **Orchestration**: Docker Compose for container management and service coordination

## Technical Stack

### Core Technologies
- **Apache Kafka 7.4.0**: High-throughput distributed streaming platform
- **Apache Spark 3.4.0**: Unified analytics engine for large-scale data processing
- **Docker & Docker Compose**: Containerization and orchestration
- **Python 3.x**: Data generation, processing, and visualization
- **Zookeeper**: Distributed coordination service for Kafka

### Libraries and Frameworks
- **PySpark**: Python API for Apache Spark
- **Kafka-Python**: Python client for Apache Kafka
- **Pandas**: Data manipulation and analysis
- **Matplotlib/Seaborn**: Statistical data visualization
- **Chart.js**: Interactive web-based charts
- **Faker**: Realistic test data generation

## Features

### Real-Time Processing
- Continuous transaction stream processing
- Sub-second fraud detection latency
- Scalable micro-batch processing (5-second intervals)
- Automatic failure recovery and checkpointing

### Fraud Detection Algorithm
- **Algorithm Type**: Rule-based classification system (NOT traditional ML models like SVM, Random Forest, etc.)
- **Feature Engineering**: 10+ engineered features including temporal, amount-based, and location risk factors
- **Framework Architecture**: Extensible design that supports future ML model integration (with pickle model loading)
- **Scoring System**: Deterministic rule evaluation with binary probability scores (0.1 for legitimate, 0.8 for fraudulent)
- **Real-time Processing**: Sub-second prediction latency using Apache Spark's distributed processing

### Data Pipeline
- Multiple data output formats (Parquet, CSV, JSON)
- Structured fraud alert system
- Data quality validation
- Comprehensive error handling

### Monitoring and Visualization
- Real-time web dashboard with live metrics
- Python-based analytics dashboard
- Kafka UI for stream monitoring
- Spark UI for job tracking and performance

## System Requirements

- Docker Desktop 4.0+
- Python 3.8+
- 8GB RAM (recommended)
- 10GB available disk space

## Installation and Setup

### 1. Clone the Repository
```bash
git clone https://github.com/tarekb21/real-time-fraud-detection.git
cd real-time-fraud-detection
```

### 2. Environment Setup
```bash
# Create Python virtual environment
python3 -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 3. Infrastructure Startup
```bash
# Start all services (Kafka, Zookeeper, Spark)
docker-compose up -d

# Verify services are running
docker-compose ps
```

### 4. Run the System
```bash
# Terminal 1: Start transaction generator
cd data_generator
python generate.py

# Terminal 2: Start fraud detection engine
./run_fraud_detection.sh

# Terminal 3: Start analytics dashboard
cd dashboard
python fraud_dashboard.py
```

## Usage

### Accessing Dashboards

1. **Web Dashboard**: Open `dashboard/web_dashboard.html` in a browser
2. **Kafka UI**: http://localhost:8080
3. **Spark UI**: http://localhost:8081
4. **Python Analytics**: Run `python fraud_dashboard.py` in terminal

### Monitoring System Performance

The system provides multiple monitoring interfaces:

- **Real-time metrics**: Transaction throughput, fraud detection rate, processing latency
- **System health**: Kafka topic lag, Spark job status, container health
- **Data quality**: Schema validation, null value detection, data freshness

### Configuration

Key configuration files:
- `docker-compose.yml`: Service definitions and networking
- `spark_processing/fraud_detector.py`: ML model parameters and thresholds
- `data_generator/generate.py`: Transaction generation parameters

## Data Flow

1. **Transaction Generation**: Realistic financial transactions with 3% fraud rate
2. **Kafka Ingestion**: Messages published to `transactions` topic
3. **Spark Processing**: Real-time stream processing and fraud detection
4. **Data Storage**: Results saved in multiple formats for different use cases
5. **Visualization**: Real-time dashboards display metrics and alerts

## Fraud Detection Algorithm Details

### Rule-Based Classification System

**Important Note**: This system uses a **rule-based approach**, not traditional machine learning algorithms like Linear Regression, SVM, or Random Forest. The fraud detection is based on deterministic business rules rather than trained statistical models.

### Feature Engineering Pipeline

The system implements comprehensive feature engineering with 10+ derived features:

**Temporal Features:**
- `hour`: Hour of transaction (0-23) for detecting unusual timing patterns
- `day_of_week`: Day of week (1-7) for weekly pattern analysis
- `is_weekend`: Binary flag for weekend transactions (higher risk)

**Amount-Based Features:**
- `amount_log`: Log-transformed transaction amount for handling skewed distributions
- `is_high_amount`: Binary flag for transactions > $1,000
- `is_low_amount`: Binary flag for micro-transactions < $10

**Location Risk Features:**
- `location_risk`: Binary flag for high-risk geographical locations (Lagos, Mumbai, Manila, Dhaka)

**Merchant Analysis Features:**
- `merchant_length`: Character length of merchant name
- `is_online_merchant`: Binary flag for online/digital merchants

### Rule-Based Fraud Detection Algorithm

The core fraud detection uses a multi-condition rule-based classifier:

```python
fraud_conditions = (
    (amount > 5000) |                                    # High-value transactions
    (hour ∈ [0,1,2,3,4,5]) |                           # Late-night transactions  
    (location_risk == 1) |                              # High-risk geographic locations
    (amount < 1) |                                      # Suspicious micro-transactions
    (is_weekend == 1) & (amount > 2000)                 # Weekend high-value transactions
)
```

**Scoring Logic:**
- Fraud Probability = 0.8 if any condition is met
- Fraud Probability = 0.1 if no conditions are met
- Classification Threshold = 0.5 (binary prediction)

### Algorithm Architecture

**Current Implementation:**
- **Algorithm Type**: Deterministic rule-based classifier (NOT ML models like SVM, Random Forest, Neural Networks)
- **Feature Space**: 10-dimensional engineered feature vector
- **Prediction Method**: Boolean logic evaluation of predefined business rules
- **Performance**: O(1) prediction time per transaction (no model training required)

**Framework for Future ML Integration:**
- **ML Model Support**: Pre-built infrastructure for scikit-learn pickle models (Linear Regression, SVM, Random Forest, etc.)
- **Model Loading**: Automatic detection of `ml_models/fraud_model.pkl`
- **Fallback Strategy**: Rule-based detection when ML model unavailable
- **Hybrid Approach**: Framework supports ensemble methods combining rules + traditional ML algorithms

## Project Structure

```
real-time-fraud-detection/
├── data_generator/          # Transaction data generation
├── kafka/                   # Kafka configuration
├── spark_processing/        # Fraud detection algorithms
├── dashboard/              # Visualization components
├── storage/                # Data output directory
├── docker/                 # Docker configurations
├── docker-compose.yml      # Service orchestration
├── requirements.txt        # Python dependencies
└── README.md              # Project documentation
```

## Dashboard Screenshots

### Real-Time Fraud Detection Dashboard
![Fraud Detection Dashboard](screenshots/dashboard.png)

*The dashboard displays real-time metrics including transaction volume, fraud detection rates, amount distributions, and system performance indicators.*

## Performance Metrics

The system achieves the following performance characteristics:

- **Throughput**: 100+ transactions per second (tested with realistic data generation)
- **Latency**: Sub-second fraud detection with 5-second micro-batch processing
- **Feature Engineering**: 10+ real-time feature computations per transaction
- **Rule Evaluation**: O(1) complexity for fraud condition checking
- **Storage Formats**: Simultaneous output to 4 different data sinks (console, Parquet, CSV, JSON alerts)
- **Availability**: Automatic checkpointing and failure recovery through Spark Streaming







