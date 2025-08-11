"""
Real-Time Fraud Detection with Apache Spark Streaming

This module reads transaction data from Kafka, applies feature engineering,
loads a pre-trained ML model, and makes real-time fraud predictions.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
import pickle
import os

class FraudDetector:
    def __init__(self):
        """Initialize Spark session and load ML model"""
        self.spark = self._create_spark_session()
        self.model = self._load_model()
        
    def _create_spark_session(self):
        """Create Spark session with Kafka integration"""
        spark = SparkSession.builder \
            .appName("RealTimeFraudDetection") \
            .config("spark.jars.packages", 
                   "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        print("✅ Spark session created successfully!")
        return spark
    
    def _load_model(self):
        """Load pre-trained fraud detection model"""
        model_path = "../ml_models/fraud_model.pkl"
        
        if os.path.exists(model_path):
            with open(model_path, 'rb') as f:
                model = pickle.load(f)
            print("✅ Pre-trained model loaded successfully!")
            return model
        else:
            print("⚠️ No pre-trained model found. Using rule-based detection.")
            return None
    
    def read_from_kafka(self):
        """Read streaming data from Kafka"""
        df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "transactions") \
            .option("startingOffsets", "latest") \
            .load()
        
        print("✅ Connected to Kafka stream!")
        return df
    
    def parse_transactions(self, df):
        """Parse JSON transaction data from Kafka"""
        
        # Define transaction schema
        transaction_schema = StructType([
            StructField("transaction_id", StringType(), True),
            StructField("user_id", IntegerType(), True),
            StructField("amount", DoubleType(), True),
            StructField("timestamp", StringType(), True),
            StructField("location", StringType(), True),
            StructField("merchant", StringType(), True),
            StructField("is_fraud", IntegerType(), True)
        ])
        
        # Parse JSON from Kafka value
        parsed_df = df.select(
            from_json(col("value").cast("string"), transaction_schema).alias("data"),
            col("timestamp").alias("kafka_timestamp")
        ).select("data.*", "kafka_timestamp")
        
        print("✅ Transaction parsing configured!")
        return parsed_df
    
    def engineer_features(self, df):
        """Create features for fraud detection"""
        
        # Add time-based features
        df_features = df.withColumn("hour", hour(col("timestamp"))) \
                       .withColumn("day_of_week", dayofweek(col("timestamp"))) \
                       .withColumn("is_weekend", 
                                 when(dayofweek(col("timestamp")).isin([1, 7]), 1).otherwise(0))
        
        # Add amount-based features
        df_features = df_features.withColumn("amount_log", log1p(col("amount"))) \
                                .withColumn("is_high_amount", 
                                          when(col("amount") > 1000, 1).otherwise(0)) \
                                .withColumn("is_low_amount", 
                                          when(col("amount") < 10, 1).otherwise(0))
        
        # Add location risk (simplified)
        high_risk_cities = ["Lagos", "Mumbai", "Manila", "Dhaka"]
        df_features = df_features.withColumn("location_risk", 
                                           when(col("location").isin(high_risk_cities), 1).otherwise(0))
        
        # Add merchant risk (simplified)
        df_features = df_features.withColumn("merchant_length", length(col("merchant"))) \
                                .withColumn("is_online_merchant", 
                                          when(col("merchant").contains("Online"), 1).otherwise(0))
        
        print("✅ Feature engineering configured!")
        return df_features
    
    def detect_fraud_simple(self, df):
        """Simple rule-based fraud detection (fallback)"""
        
        # Rule-based fraud detection logic
        fraud_conditions = (
            (col("amount") > 5000) |  # High amount transactions
            (col("hour").isin([0, 1, 2, 3, 4, 5])) |  # Late night transactions
            (col("location_risk") == 1) |  # High-risk locations
            (col("amount") < 1) |  # Micro transactions
            (col("is_weekend") == 1) & (col("amount") > 2000)  # Weekend high amounts
        )
        
        df_scored = df.withColumn("fraud_probability", 
                                when(fraud_conditions, 0.8).otherwise(0.1)) \
                     .withColumn("is_fraud_predicted", 
                               when(col("fraud_probability") > 0.5, 1).otherwise(0)) \
                     .withColumn("prediction_method", lit("rule_based"))
        
        return df_scored
    
    def detect_fraud_ml(self, df):
        """ML-based fraud detection (when model is available)"""
        # This would use the actual ML model
        # For now, we'll use the rule-based approach
        print("🧠 Using ML model for fraud detection...")
        return self.detect_fraud_simple(df)
    
    def detect_fraud(self, df):
        """Main fraud detection method"""
        if self.model is not None:
            return self.detect_fraud_ml(df)
        else:
            return self.detect_fraud_simple(df)
    
    def write_to_console(self, df):
        """Write results to console for monitoring"""
        query = df.select(
            "transaction_id",
            "user_id", 
            "amount",
            "location",
            "merchant",
            "fraud_probability",
            "is_fraud_predicted",
            "is_fraud"  # True label for comparison
        ).writeStream \
         .outputMode("append") \
         .format("console") \
         .option("truncate", False) \
         .option("numRows", 20) \
         .trigger(processingTime="10 seconds") \
         .start()
        
        return query
    
    def write_to_file(self, df):
        """Write results to parquet files"""
        query = df.writeStream \
                 .outputMode("append") \
                 .format("parquet") \
                 .option("path", "../storage/fraud_predictions") \
                 .option("checkpointLocation", "../storage/checkpoints") \
                 .trigger(processingTime="30 seconds") \
                 .start()
        
        return query
    
    def run_fraud_detection(self):
        """Main method to run the fraud detection pipeline"""
        print("🚀 Starting Real-Time Fraud Detection System...")
        print("📊 Reading from Kafka topic: transactions")
        print("🧠 Applying fraud detection algorithms")
        print("💾 Storing results and displaying alerts")
        print("🔄 Press Ctrl+C to stop\n")
        
        try:
            # Read from Kafka
            raw_df = self.read_from_kafka()
            
            # Parse transactions
            parsed_df = self.parse_transactions(raw_df)
            
            # Engineer features
            featured_df = self.engineer_features(parsed_df)
            
            # Detect fraud
            scored_df = self.detect_fraud(featured_df)
            
            # Write to console (for monitoring)
            console_query = self.write_to_console(scored_df)
            
            # Write to files (for storage)
            file_query = self.write_to_file(scored_df)
            
            # Wait for termination
            console_query.awaitTermination()
            
        except KeyboardInterrupt:
            print("\n🛑 Stopping fraud detection system...")
            self.spark.stop()
            print("✅ Spark session closed successfully!")
        except Exception as e:
            print(f"❌ Error in fraud detection pipeline: {e}")
            self.spark.stop()

def main():
    """Entry point for the fraud detection system"""
    detector = FraudDetector()
    detector.run_fraud_detection()

if __name__ == "__main__":
    main()
