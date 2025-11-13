#!/usr/bin/env python3
"""
Create demo Iceberg tables in Unity Catalog

This script creates example tables that will be imported into
the GitOps Schema Federation Manager.
"""

import os
import sys
import json
import time
import requests
from typing import Dict, List


class UnityCatalogSetup:
    """Setup Unity Catalog with demo data"""

    def __init__(self, uc_url: str):
        self.uc_url = uc_url.rstrip('/')
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json'
        })

    def wait_for_unity_catalog(self, max_retries=30):
        """Wait for Unity Catalog to be ready"""
        print("Waiting for Unity Catalog to be ready...")

        for i in range(max_retries):
            try:
                response = self.session.get(f"{self.uc_url}/api/2.1/unity-catalog/catalogs")
                if response.status_code == 200:
                    print("✓ Unity Catalog is ready")
                    return True
            except requests.exceptions.RequestException:
                pass

            print(f"  Attempt {i+1}/{max_retries}...")
            time.sleep(2)

        print("✗ Unity Catalog failed to start")
        return False

    def create_catalog(self, name: str, comment: str = "") -> bool:
        """Create a catalog"""
        url = f"{self.uc_url}/api/2.1/unity-catalog/catalogs"

        payload = {
            'name': name,
            'comment': comment,
            'properties': {}
        }

        try:
            response = self.session.post(url, json=payload)
            if response.status_code in [200, 201]:
                print(f"✓ Created catalog: {name}")
                return True
            elif response.status_code == 409:
                print(f"  Catalog already exists: {name}")
                return True
            else:
                print(f"✗ Failed to create catalog {name}: {response.text}")
                return False
        except Exception as e:
            print(f"✗ Error creating catalog {name}: {e}")
            return False

    def create_schema(self, catalog: str, schema: str, comment: str = "") -> bool:
        """Create a schema (namespace)"""
        url = f"{self.uc_url}/api/2.1/unity-catalog/schemas"

        payload = {
            'name': schema,
            'catalog_name': catalog,
            'comment': comment,
            'properties': {}
        }

        try:
            response = self.session.post(url, json=payload)
            if response.status_code in [200, 201]:
                print(f"✓ Created schema: {catalog}.{schema}")
                return True
            elif response.status_code == 409:
                print(f"  Schema already exists: {catalog}.{schema}")
                return True
            else:
                print(f"✗ Failed to create schema {catalog}.{schema}: {response.text}")
                return False
        except Exception as e:
            print(f"✗ Error creating schema {catalog}.{schema}: {e}")
            return False

    def create_table(self, catalog: str, schema: str, table: str,
                     columns: List[Dict], comment: str = "") -> bool:
        """Create a table"""
        url = f"{self.uc_url}/api/2.1/unity-catalog/tables"

        payload = {
            'name': table,
            'catalog_name': catalog,
            'schema_name': schema,
            'table_type': 'MANAGED',
            'data_source_format': 'DELTA',  # Using Delta Lake format
            'columns': columns,
            'comment': comment,
            'properties': {
                'created_by': 'demo-setup',
                'purpose': 'demonstration'
            }
        }

        try:
            response = self.session.post(url, json=payload)
            if response.status_code in [200, 201]:
                print(f"✓ Created table: {catalog}.{schema}.{table}")
                return True
            elif response.status_code == 409:
                print(f"  Table already exists: {catalog}.{schema}.{table}")
                return True
            else:
                print(f"✗ Failed to create table {catalog}.{schema}.{table}: {response.text}")
                return False
        except Exception as e:
            print(f"✗ Error creating table {catalog}.{schema}.{table}: {e}")
            return False


def create_demo_environment():
    """Create complete demo environment"""
    uc_url = os.getenv('UC_URL', 'http://unity-catalog:8080')

    setup = UnityCatalogSetup(uc_url)

    # Wait for Unity Catalog
    if not setup.wait_for_unity_catalog():
        sys.exit(1)

    print("\n" + "="*60)
    print("Creating Demo Environment in Unity Catalog")
    print("="*60 + "\n")

    # Create main catalog
    setup.create_catalog('main', 'Main production catalog')

    # Create bronze schema (raw data)
    setup.create_schema('main', 'bronze', 'Bronze layer - raw ingested data')

    # Create users table (matches Kafka user-events schema)
    users_columns = [
        {
            'name': 'user_id',
            'type_name': 'string',
            'type_text': 'string',
            'type_json': '{"type":"string"}',
            'position': 0,
            'nullable': False,
            'comment': 'Unique user identifier'
        },
        {
            'name': 'email',
            'type_name': 'string',
            'type_text': 'string',
            'type_json': '{"type":"string"}',
            'position': 1,
            'nullable': True,
            'comment': 'User email address'
        },
        {
            'name': 'first_name',
            'type_name': 'string',
            'type_text': 'string',
            'type_json': '{"type":"string"}',
            'position': 2,
            'nullable': True,
            'comment': 'User first name'
        },
        {
            'name': 'last_name',
            'type_name': 'string',
            'type_text': 'string',
            'type_json': '{"type":"string"}',
            'position': 3,
            'nullable': True,
            'comment': 'User last name'
        },
        {
            'name': 'created_at',
            'type_name': 'timestamp',
            'type_text': 'timestamp',
            'type_json': '{"type":"timestamp"}',
            'position': 4,
            'nullable': False,
            'comment': 'Account creation timestamp'
        },
        {
            'name': 'updated_at',
            'type_name': 'timestamp',
            'type_text': 'timestamp',
            'type_json': '{"type":"timestamp"}',
            'position': 5,
            'nullable': False,
            'comment': 'Last update timestamp'
        },
        {
            'name': 'is_active',
            'type_name': 'boolean',
            'type_text': 'boolean',
            'type_json': '{"type":"boolean"}',
            'position': 6,
            'nullable': False,
            'comment': 'Active status flag'
        }
    ]

    setup.create_table(
        catalog='main',
        schema='bronze',
        table='users',
        columns=users_columns,
        comment='User dimension table - imported from Kafka CDC stream'
    )

    # Create orders table
    orders_columns = [
        {
            'name': 'order_id',
            'type_name': 'string',
            'type_text': 'string',
            'type_json': '{"type":"string"}',
            'position': 0,
            'nullable': False,
            'comment': 'Unique order identifier'
        },
        {
            'name': 'user_id',
            'type_name': 'string',
            'type_text': 'string',
            'type_json': '{"type":"string"}',
            'position': 1,
            'nullable': False,
            'comment': 'User who placed the order'
        },
        {
            'name': 'order_total',
            'type_name': 'decimal(10,2)',
            'type_text': 'decimal(10,2)',
            'type_json': '{"type":"decimal","precision":10,"scale":2}',
            'position': 2,
            'nullable': False,
            'comment': 'Total order amount'
        },
        {
            'name': 'order_status',
            'type_name': 'string',
            'type_text': 'string',
            'type_json': '{"type":"string"}',
            'position': 3,
            'nullable': False,
            'comment': 'Order status (PENDING, SHIPPED, DELIVERED, CANCELLED)'
        },
        {
            'name': 'created_at',
            'type_name': 'timestamp',
            'type_text': 'timestamp',
            'type_json': '{"type":"timestamp"}',
            'position': 4,
            'nullable': False,
            'comment': 'Order creation timestamp'
        },
        {
            'name': 'items',
            'type_name': 'array<string>',
            'type_text': 'array<string>',
            'type_json': '{"type":"array","elementType":"string"}',
            'position': 5,
            'nullable': True,
            'comment': 'List of item IDs in the order'
        }
    ]

    setup.create_table(
        catalog='main',
        schema='bronze',
        table='orders',
        columns=orders_columns,
        comment='Order fact table - imported from Kafka order-events stream'
    )

    # Create silver schema (cleaned/enriched data)
    setup.create_schema('main', 'silver', 'Silver layer - cleaned and enriched data')

    # Create enriched users table
    users_enriched_columns = users_columns + [
        {
            'name': 'total_orders',
            'type_name': 'long',
            'type_text': 'bigint',
            'type_json': '{"type":"long"}',
            'position': 7,
            'nullable': True,
            'comment': 'Total number of orders'
        },
        {
            'name': 'lifetime_value',
            'type_name': 'decimal(10,2)',
            'type_text': 'decimal(10,2)',
            'type_json': '{"type":"decimal","precision":10,"scale":2}',
            'position': 8,
            'nullable': True,
            'comment': 'Customer lifetime value'
        }
    ]

    setup.create_table(
        catalog='main',
        schema='silver',
        table='users_enriched',
        columns=users_enriched_columns,
        comment='Enriched user data with aggregated metrics'
    )

    # Create analytics catalog
    setup.create_catalog('analytics', 'Analytics and reporting catalog')
    setup.create_schema('analytics', 'reports', 'Reporting tables')

    # Create user analytics table
    user_analytics_columns = [
        {
            'name': 'user_id',
            'type_name': 'string',
            'type_text': 'string',
            'type_json': '{"type":"string"}',
            'position': 0,
            'nullable': False,
            'comment': 'User identifier'
        },
        {
            'name': 'event_date',
            'type_name': 'date',
            'type_text': 'date',
            'type_json': '{"type":"date"}',
            'position': 1,
            'nullable': False,
            'comment': 'Event date'
        },
        {
            'name': 'event_count',
            'type_name': 'long',
            'type_text': 'bigint',
            'type_json': '{"type":"long"}',
            'position': 2,
            'nullable': False,
            'comment': 'Number of events'
        },
        {
            'name': 'event_types',
            'type_name': 'map<string,long>',
            'type_text': 'map<string,bigint>',
            'type_json': '{"type":"map","keyType":"string","valueType":"long"}',
            'position': 3,
            'nullable': True,
            'comment': 'Event type breakdown'
        }
    ]

    setup.create_table(
        catalog='analytics',
        schema='reports',
        table='user_activity_daily',
        columns=user_analytics_columns,
        comment='Daily user activity aggregations'
    )

    print("\n" + "="*60)
    print("Demo Environment Created Successfully!")
    print("="*60)
    print("\nCreated Catalogs:")
    print("  - main (bronze, silver)")
    print("  - analytics (reports)")
    print("\nCreated Tables:")
    print("  - main.bronze.users")
    print("  - main.bronze.orders")
    print("  - main.silver.users_enriched")
    print("  - analytics.reports.user_activity_daily")
    print("\n" + "="*60 + "\n")


if __name__ == '__main__':
    create_demo_environment()
