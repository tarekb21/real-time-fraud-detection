#!/usr/bin/env python3
"""
Real-Time Fraud Detection Dashboard

This dashboard provides visual analytics and monitoring for the fraud detection system.
It reads from the generated CSV files and creates interactive charts.
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import glob
import os
import time
import json
from pathlib import Path

class FraudDashboard:
    def __init__(self):
        """Initialize the fraud dashboard"""
        self.base_path = "../storage"
        self.csv_path = f"{self.base_path}/fraud_results_csv"
        self.alerts_path = f"{self.base_path}/fraud_alerts"
        
        # Set up plotting style
        plt.style.use('seaborn-v0_8')
        sns.set_palette("husl")
        
    def load_latest_data(self):
        """Load the latest fraud detection results"""
        try:
            # Find all CSV files
            csv_files = glob.glob(f"{self.csv_path}/**/*.csv", recursive=True)
            
            if not csv_files:
                print("No CSV data files found yet. Waiting for data...")
                return None
            
            # Read and combine all CSV files
            dfs = []
            for file in csv_files:
                try:
                    df = pd.read_csv(file)
                    dfs.append(df)
                except Exception as e:
                    print(f"Error reading {file}: {e}")
                    continue
            
            if not dfs:
                return None
                
            # Combine all dataframes
            combined_df = pd.concat(dfs, ignore_index=True)
            
            # Clean and process data
            combined_df['amount'] = pd.to_numeric(combined_df['amount'], errors='coerce')
            combined_df['fraud_probability'] = pd.to_numeric(combined_df['fraud_probability'], errors='coerce')
            combined_df['processed_time'] = pd.to_datetime(combined_df['processed_time'], errors='coerce')
            
            return combined_df
            
        except Exception as e:
            print(f"Error loading data: {e}")
            return None
    
    def load_fraud_alerts(self):
        """Load fraud alerts from JSON files"""
        try:
            alert_files = glob.glob(f"{self.alerts_path}/**/*.json", recursive=True)
            
            if not alert_files:
                return None
            
            alerts = []
            for file in alert_files:
                try:
                    with open(file, 'r') as f:
                        for line in f:
                            alert = json.loads(line.strip())
                            alerts.append(alert)
                except Exception as e:
                    print(f"Error reading alert file {file}: {e}")
                    continue
            
            if alerts:
                return pd.DataFrame(alerts)
            return None
            
        except Exception as e:
            print(f"Error loading alerts: {e}")
            return None
    
    def create_fraud_summary(self, df):
        """Create fraud detection summary statistics"""
        if df is None or df.empty:
            print("No data available for summary")
            return
        
        print("\n" + "="*60)
        print(" FRAUD DETECTION DASHBOARD ")
        print("="*60)
        
        total_transactions = len(df)
        fraud_detected = len(df[df['is_fraud_predicted'] == 1])
        actual_fraud = len(df[df['is_fraud'] == 1])
        
        print(f"📊 TRANSACTION SUMMARY:")
        print(f"   Total Transactions: {total_transactions:,}")
        print(f"   Fraud Detected: {fraud_detected:,} ({fraud_detected/total_transactions*100:.1f}%)")
        print(f"   Actual Fraud: {actual_fraud:,} ({actual_fraud/total_transactions*100:.1f}%)")
        
        if fraud_detected > 0 and actual_fraud > 0:
            # Calculate precision and recall
            true_positives = len(df[(df['is_fraud_predicted'] == 1) & (df['is_fraud'] == 1)])
            precision = true_positives / fraud_detected if fraud_detected > 0 else 0
            recall = true_positives / actual_fraud if actual_fraud > 0 else 0
            
            print(f"\n MODEL PERFORMANCE:")
            print(f"   Precision: {precision:.2f} ({true_positives}/{fraud_detected})")
            print(f"   Recall: {recall:.2f} ({true_positives}/{actual_fraud})")
        
        # Amount statistics
        total_amount = df['amount'].sum()
        fraud_amount = df[df['is_fraud_predicted'] == 1]['amount'].sum()
        
        print(f"\n💰 FINANCIAL IMPACT:")
        print(f"   Total Transaction Volume: ${total_amount:,.2f}")
        print(f"   Flagged Fraud Amount: ${fraud_amount:,.2f}")
        print(f"   Average Transaction: ${df['amount'].mean():.2f}")
        print(f"   Average Fraud Amount: ${df[df['is_fraud_predicted'] == 1]['amount'].mean():.2f}")
        
    def create_visualizations(self, df):
        """Create fraud detection visualizations"""
        if df is None or df.empty:
            print("No data available for visualizations")
            return
        
        # Create figure with subplots
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('Real-Time Fraud Detection Analytics', fontsize=16, fontweight='bold')
        
        # 1. Fraud vs Legitimate Transactions
        fraud_counts = df['is_fraud_predicted'].value_counts()
        labels = ['Legitimate', 'Fraudulent']
        colors = ['lightgreen', 'lightcoral']
        
        axes[0, 0].pie(fraud_counts.values, labels=labels, colors=colors, autopct='%1.1f%%')
        axes[0, 0].set_title('Fraud Detection Results')
        
        # 2. Transaction Amount Distribution
        axes[0, 1].hist([df[df['is_fraud_predicted'] == 0]['amount'], 
                        df[df['is_fraud_predicted'] == 1]['amount']], 
                       bins=30, alpha=0.7, label=['Legitimate', 'Fraudulent'])
        axes[0, 1].set_xlabel('Transaction Amount ($)')
        axes[0, 1].set_ylabel('Frequency')
        axes[0, 1].set_title('Transaction Amount Distribution')
        axes[0, 1].legend()
        
        # 3. Fraud Probability Distribution
        axes[1, 0].hist(df['fraud_probability'], bins=20, alpha=0.7, color='orange')
        axes[1, 0].set_xlabel('Fraud Probability')
        axes[1, 0].set_ylabel('Frequency')
        axes[1, 0].set_title('Fraud Probability Distribution')
        
        # 4. Top Locations with Fraud
        if 'location' in df.columns:
            fraud_by_location = df[df['is_fraud_predicted'] == 1]['location'].value_counts().head(10)
            if not fraud_by_location.empty:
                fraud_by_location.plot(kind='bar', ax=axes[1, 1])
                axes[1, 1].set_title('Top 10 Locations with Fraud')
                axes[1, 1].set_xlabel('Location')
                axes[1, 1].set_ylabel('Fraud Count')
                axes[1, 1].tick_params(axis='x', rotation=45)
        
        plt.tight_layout()
        
        # Save the plot
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        plot_path = f"../storage/fraud_dashboard_{timestamp}.png"
        plt.savefig(plot_path, dpi=300, bbox_inches='tight')
        print(f"\n📈 Dashboard saved as: {plot_path}")
        plt.show()
    
    def monitor_alerts(self):
        """Monitor and display recent fraud alerts"""
        alerts_df = self.load_fraud_alerts()
        
        if alerts_df is not None and not alerts_df.empty:
            print(f"\n RECENT FRAUD ALERTS ({len(alerts_df)} total):")
            print("-" * 50)
            
            # Show last 5 alerts
            recent_alerts = alerts_df.tail(5)
            for _, alert in recent_alerts.iterrows():
                print(f"   Alert: Transaction {alert.get('transaction_id', 'N/A')}")
                print(f"   User: {alert.get('user_id', 'N/A')}")
                print(f"   Amount: ${alert.get('amount', 0):.2f}")
                print(f"   Location: {alert.get('location', 'N/A')}")
                print(f"   Merchant: {alert.get('merchant', 'N/A')}")
                print(f"   Risk Score: {alert.get('fraud_probability', 0):.2f}")
                print()
        else:
            print("\n✅ No fraud alerts at this time")
    
    def run_dashboard(self, refresh_seconds=30):
        """Run the dashboard with auto-refresh"""
        print("Starting Fraud Detection Dashboard...")
        print(f"Monitoring data in: {self.csv_path}")
        print(f"Refresh interval: {refresh_seconds} seconds")
        print("Press Ctrl+C to stop")
        
        try:
            while True:
                # Clear screen (works on most terminals)
                os.system('clear' if os.name == 'posix' else 'cls')
                
                print(f"🕐 Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                
                # Load and analyze data
                df = self.load_latest_data()
                
                if df is not None and not df.empty:
                    self.create_fraud_summary(df)
                    self.monitor_alerts()
                    
                    # Create visualizations every 5 refreshes
                    if hasattr(self, 'refresh_count'):
                        self.refresh_count += 1
                    else:
                        self.refresh_count = 1
                    
                    if self.refresh_count % 5 == 0:
                        print("\n Generating visualizations...")
                        self.create_visualizations(df)
                else:
                    print("\n⏳ Waiting for fraud detection data...")
                    print("Make sure the Spark fraud detector is running!")
                
                print(f"\n🔄 Next refresh in {refresh_seconds} seconds...")
                time.sleep(refresh_seconds)
                
        except KeyboardInterrupt:
            print("\n\n Dashboard stopped by user")
        except Exception as e:
            print(f"\n Dashboard error: {e}")

def main():
    """Main function to run the dashboard"""
    dashboard = FraudDashboard()
    dashboard.run_dashboard(refresh_seconds=20)

if __name__ == "__main__":
    main()
