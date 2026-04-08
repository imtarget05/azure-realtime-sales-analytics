"""
Simulate Data Drift - Create synthetic drift data for testing MLOps workflow

This script helps test the automated drift detection and retrain pipeline by
injecting synthetic drift data into the database.

Usage:
    python scripts/simulate_drift.py --drift-type price_inflation --severity medium
    python scripts/simulate_drift.py --drift-type data_corruption --severity high
    python scripts/simulate_drift.py --drift-type category_shift --severity low
"""

import argparse
import random
import sys
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pyodbc

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import SQL_SERVER, SQL_DATABASE, SQL_USERNAME, SQL_PASSWORD, SQL_DRIVER

SQL_CONNECTION_STRING = (
    f"DRIVER={SQL_DRIVER};SERVER={SQL_SERVER};DATABASE={SQL_DATABASE};"
    f"UID={SQL_USERNAME};PWD={SQL_PASSWORD};"
    "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
)


class DriftSimulator:
    """Simulate various types of data drift"""
    
    def __init__(self, drift_type: str, severity: str):
        self.drift_type = drift_type
        self.severity = severity
        self.severity_multiplier = {
            'low': 0.2,
            'medium': 0.5,
            'high': 0.8
        }[severity]
        
    def generate_drift_data(self, num_samples: int = 1000) -> pd.DataFrame:
        """Generate synthetic drift data based on drift type"""
        
        if self.drift_type == 'price_inflation':
            return self._generate_price_inflation(num_samples)
        elif self.drift_type == 'data_corruption':
            return self._generate_data_corruption(num_samples)
        elif self.drift_type == 'category_shift':
            return self._generate_category_shift(num_samples)
        else:
            raise ValueError(f"Unknown drift type: {self.drift_type}")
    
    def _generate_price_inflation(self, num_samples: int) -> pd.DataFrame:
        """Simulate price inflation - UnitPrice increases significantly"""
        
        # Base realistic product data
        product_ids = np.random.choice(range(1, 501), size=num_samples)
        quantities = np.random.choice(range(1, 21), size=num_samples)
        
        # Original price distribution: mean=50, std=30
        # Drifted: increase by severity multiplier
        base_prices = np.random.gamma(shape=2, scale=25, size=num_samples)
        inflation_factor = 1 + (self.severity_multiplier * 1.5)  # Up to 2.2x for high
        drifted_prices = base_prices * inflation_factor
        
        # Add timestamps (recent data)
        end_time = datetime.now()
        timestamps = [
            end_time - timedelta(hours=random.randint(0, 48))
            for _ in range(num_samples)
        ]
        
        df = pd.DataFrame({
            'ProductID': product_ids,
            'Quantity': quantities,
            'UnitPrice': np.round(drifted_prices, 2),
            'EventTime': timestamps,
            'CustomerID': np.random.choice(range(1, 1001), size=num_samples),
            'Region': np.random.choice(['North', 'South', 'East', 'West'], size=num_samples),
        })
        
        print(f"✅ Generated {num_samples} samples with price inflation")
        print(f"   Price range: ${df['UnitPrice'].min():.2f} - ${df['UnitPrice'].max():.2f}")
        print(f"   Mean price: ${df['UnitPrice'].mean():.2f} (inflation factor: {inflation_factor:.2f}x)")
        
        return df
    
    def _generate_data_corruption(self, num_samples: int) -> pd.DataFrame:
        """Simulate data corruption - noisy, outliers, invalid values"""
        
        # Generate base data
        df = self._generate_normal_data(num_samples)
        
        # Corrupt based on severity
        num_corrupt = int(num_samples * self.severity_multiplier)
        corrupt_indices = np.random.choice(df.index, size=num_corrupt, replace=False)

        third = num_corrupt // 3
        idx_outliers = corrupt_indices[:third]
        idx_low = corrupt_indices[third: 2 * third]
        idx_qty = corrupt_indices[2 * third:]

        # Add extreme outliers
        df.loc[idx_outliers, 'UnitPrice'] = np.random.uniform(1000, 5000, len(idx_outliers))

        # Add near-zero prices (data error)
        df.loc[idx_low, 'UnitPrice'] = np.random.uniform(0.01, 1.0, len(idx_low))

        # Add extreme quantities
        df.loc[idx_qty, 'Quantity'] = np.random.randint(100, 500, len(idx_qty))
        
        print(f"✅ Generated {num_samples} samples with data corruption")
        print(f"   Corrupted records: {num_corrupt} ({self.severity_multiplier*100:.0f}%)")
        print(f"   Price outliers: {(df['UnitPrice'] > 1000).sum()}")
        print(f"   Invalid prices: {(df['UnitPrice'] < 1).sum()}")
        
        return df
    
    def _generate_category_shift(self, num_samples: int) -> pd.DataFrame:
        """Simulate category shift - distribution of ProductIDs changes"""
        
        # Original: uniform distribution across 500 products
        # Drifted: concentrate on luxury items (high ProductID)
        
        shift_amount = self.severity_multiplier
        
        # Biased distribution towards high product IDs
        if shift_amount > 0.5:
            # High severity: strong bias towards luxury (ProductID > 400)
            product_ids = np.random.choice(
                range(400, 501),
                size=int(num_samples * 0.7),
                replace=True
            )
            remaining = np.random.choice(range(1, 400), size=num_samples - len(product_ids))
            product_ids = np.concatenate([product_ids, remaining])
        else:
            # Low-medium severity: moderate bias
            weights = np.linspace(0.5, 1.5, 500)  # Increasing weights
            weights = weights / weights.sum()
            product_ids = np.random.choice(range(1, 501), size=num_samples, p=weights)
        
        np.random.shuffle(product_ids)
        
        quantities = np.random.choice(range(1, 21), size=num_samples)
        prices = np.random.gamma(shape=2, scale=25, size=num_samples)
        
        end_time = datetime.now()
        timestamps = [
            end_time - timedelta(hours=random.randint(0, 48))
            for _ in range(num_samples)
        ]
        
        df = pd.DataFrame({
            'ProductID': product_ids,
            'Quantity': quantities,
            'UnitPrice': np.round(prices, 2),
            'EventTime': timestamps,
            'CustomerID': np.random.choice(range(1, 1001), size=num_samples),
            'Region': np.random.choice(['North', 'South', 'East', 'West'], size=num_samples),
        })
        
        print(f"✅ Generated {num_samples} samples with category shift")
        print(f"   ProductID distribution shifted towards luxury items")
        print(f"   High-end products (>400): {(df['ProductID'] > 400).sum()} ({(df['ProductID'] > 400).sum()/len(df)*100:.1f}%)")
        
        return df
    
    def _generate_normal_data(self, num_samples: int) -> pd.DataFrame:
        """Generate normal (non-drifted) data for comparison"""
        
        product_ids = np.random.choice(range(1, 501), size=num_samples)
        quantities = np.random.choice(range(1, 21), size=num_samples)
        prices = np.random.gamma(shape=2, scale=25, size=num_samples)
        
        end_time = datetime.now()
        timestamps = [
            end_time - timedelta(hours=random.randint(0, 48))
            for _ in range(num_samples)
        ]
        
        return pd.DataFrame({
            'ProductID': product_ids,
            'Quantity': quantities,
            'UnitPrice': np.round(prices, 2),
            'EventTime': timestamps,
            'CustomerID': np.random.choice(range(1, 1001), size=num_samples),
            'Region': np.random.choice(['North', 'South', 'East', 'West'], size=num_samples),
        })


def inject_data_to_sql(df: pd.DataFrame, table_name: str = 'SalesEvents'):
    """Insert drift data into SQL database"""
    
    print(f"\n📤 Injecting {len(df)} records into SQL table '{table_name}'...")
    
    try:
        conn = pyodbc.connect(SQL_CONNECTION_STRING)
        cursor = conn.cursor()
        
        # Insert data row by row
        insert_query = f"""
        INSERT INTO {table_name} (ProductID, Quantity, UnitPrice, EventTime, CustomerID, Region)
        VALUES (?, ?, ?, ?, ?, ?)
        """
        
        inserted = 0
        for _, row in df.iterrows():
            try:
                cursor.execute(
                    insert_query,
                    int(row['ProductID']),
                    int(row['Quantity']),
                    float(row['UnitPrice']),
                    row['EventTime'],
                    int(row['CustomerID']),
                    str(row['Region'])
                )
                inserted += 1
            except Exception as e:
                print(f"⚠️ Failed to insert row: {e}")
                continue
        
        conn.commit()
        print(f"✅ Successfully injected {inserted}/{len(df)} records")
        
        # Verify
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        total_count = cursor.fetchone()[0]
        print(f"📊 Total records in {table_name}: {total_count}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Error injecting data to SQL: {e}")
        raise


def main():
    parser = argparse.ArgumentParser(
        description='Simulate data drift for testing MLOps workflow',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Simulate moderate price inflation
  python simulate_drift.py --drift-type price_inflation --severity medium
  
  # Simulate severe data corruption
  python simulate_drift.py --drift-type data_corruption --severity high
  
  # Simulate slight category shift
  python simulate_drift.py --drift-type category_shift --severity low
  
  # Generate without injecting to SQL (dry-run)
  python simulate_drift.py --drift-type price_inflation --severity medium --dry-run
        """
    )
    
    parser.add_argument(
        '--drift-type',
        type=str,
        required=True,
        choices=['price_inflation', 'data_corruption', 'category_shift'],
        help='Type of drift to simulate'
    )
    
    parser.add_argument(
        '--severity',
        type=str,
        required=True,
        choices=['low', 'medium', 'high'],
        help='Severity of drift'
    )
    
    parser.add_argument(
        '--num-samples',
        type=int,
        default=1000,
        help='Number of drift samples to generate (default: 1000)'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Generate data without injecting to SQL'
    )
    
    parser.add_argument(
        '--output',
        type=str,
        help='Save generated data to CSV file'
    )
    
    args = parser.parse_args()
    
    print("="*60)
    print("🧪 Data Drift Simulator")
    print("="*60)
    print(f"Drift type: {args.drift_type}")
    print(f"Severity: {args.severity}")
    print(f"Samples: {args.num_samples}")
    print(f"Dry run: {args.dry_run}")
    print("="*60)
    print()
    
    # Generate drift data
    simulator = DriftSimulator(args.drift_type, args.severity)
    df = simulator.generate_drift_data(args.num_samples)
    
    # Save to CSV if requested
    if args.output:
        df.to_csv(args.output, index=False)
        print(f"\n💾 Data saved to: {args.output}")
    
    # Inject to SQL unless dry-run
    if not args.dry_run:
        inject_data_to_sql(df)
    else:
        print("\n⚠️ Dry run mode: Data NOT injected to SQL")
        print(f"   Preview of generated data:")
        print(df.head(10))
    
    print("\n" + "="*60)
    print("✅ Drift simulation complete!")
    print("="*60)
    print("\nNext steps:")
    print("1. Run drift detection: python ml/drift_monitor.py")
    print("2. Check dashboard: http://localhost:5000/dashboard")
    print("3. Verify notifications in Slack/Teams")
    print("="*60)


if __name__ == '__main__':
    main()
