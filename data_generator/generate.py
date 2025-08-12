from faker import Faker
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable, KafkaError
import json
import random
import time
import sys

fake = Faker()

def create_kafka_producer(max_retries=5, retry_delay=5):
    """
    Create a Kafka producer with retry logic.
    This helps handle cases where Kafka isn't ready yet.
    """
    for attempt in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers='localhost:9092',
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                # Add some resilience configurations
                retries=3,
                max_in_flight_requests_per_connection=1,
                acks='all'  # Wait for all replicas to acknowledge
            )
            print("Successfully connected to Kafka!")
            return producer
        except NoBrokersAvailable:
            print(f"Attempt {attempt + 1}/{max_retries}: Kafka broker not available")
            if attempt < max_retries - 1:
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                print("Make sure Kafka is running: docker-compose up -d")
                sys.exit(1)
    return None

# Initialize Kafka producer with retry logic
producer = create_kafka_producer()

def generate_transaction():
    """
    Generate financial transactions.
    
    Returns a dictionary with:
    - transaction_id: Unique identifier for the transaction
    - user_id: Random user ID (simulating different customers)
    - amount: Transaction amount ($5 to $10,000)
    - timestamp: When the transaction occurred
    - location: Where the transaction happened
    - merchant: Which business processed the transaction
    - is_fraud: Whether this is a fraudulent transaction (3% chance)
    """
    return {
        "transaction_id": fake.uuid4(),
        "user_id": random.randint(1000, 9999),
        "amount": round(random.uniform(5.0, 10000.0), 2),
        "timestamp": fake.iso8601(),
        "location": fake.city(),
        "merchant": fake.company(),
        "is_fraud": random.choices([0, 1], weights=[0.97, 0.03])[0]  # ~3% fraud
    }

def main():
    """
    Main loop that generates and sends transactions to Kafka.
    """
    print("Starting transaction generator...")
    print("Generating ~3% fraudulent transactions")
    print("Sending 1 transaction per second")
    print("Press Ctrl+C to stop\n")
    
    transaction_count = 0
    fraud_count = 0
    
    try:
        while True:
            transaction = generate_transaction()
            
            # Track statistics
            transaction_count += 1
            if transaction['is_fraud']:
                fraud_count += 1
            
            # Send to Kafka
            try:
                producer.send('transactions', value=transaction)
                
                # Display transaction info
                status = "FRAUD" if transaction['is_fraud'] else "LEGIT"
                print(f"Transaction #{transaction_count} | {status} | "
                      f"${transaction['amount']:.2f} | {transaction['merchant']} | "
                      f"{transaction['location']}")
                
                # Show fraud rate every 50 transactions
                if transaction_count % 50 == 0:
                    fraud_rate = (fraud_count / transaction_count) * 100
                    print(f"\nStats: {transaction_count} transactions sent, "
                          f"fraud rate: {fraud_rate:.1f}%\n")
                
            except KafkaError as e:
                print(f"Failed to send transaction: {e}")
            
            time.sleep(1)  # 1 transaction per second
            
    except KeyboardInterrupt:
        print(f"\n\nStopping generator...")
        print(f"Final stats: {transaction_count} transactions, "
              f"{fraud_count} fraudulent ({(fraud_count/transaction_count)*100:.1f}%)")
        producer.close()

if __name__ == "__main__":
    main()
