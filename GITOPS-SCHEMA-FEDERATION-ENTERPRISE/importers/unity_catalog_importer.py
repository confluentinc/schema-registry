"""
Unity Catalog Schema Importer

This module imports table schemas from Unity Catalog and converts them
to Avro format for storage in SCHEMASTORE.
"""

import json
import hashlib
import requests
from typing import Dict, List, Optional, Any
from datetime import datetime
from dataclasses import dataclass, asdict
from enum import Enum


class IcebergType(Enum):
    """Iceberg primitive types"""
    BOOLEAN = "boolean"
    INT = "int"
    LONG = "long"
    FLOAT = "float"
    DOUBLE = "double"
    DECIMAL = "decimal"
    DATE = "date"
    TIME = "time"
    TIMESTAMP = "timestamp"
    TIMESTAMPTZ = "timestamptz"
    STRING = "string"
    UUID = "uuid"
    FIXED = "fixed"
    BINARY = "binary"
    STRUCT = "struct"
    LIST = "list"
    MAP = "map"


@dataclass
class TableInfo:
    """Unity Catalog table information"""
    catalog_name: str
    schema_name: str
    table_name: str
    table_type: str
    data_source_format: str
    storage_location: str
    owner: str
    created_at: str
    updated_at: str
    columns: List[Dict[str, Any]]
    properties: Dict[str, str]


@dataclass
class SchemaMetadata:
    """Extended metadata for imported schemas"""
    schema_id: int
    subject: str
    version: int
    context: str
    schema_type: str
    platform: str
    source_info: Dict[str, Any]
    compatibility_mode: str
    deployment_targets: Dict[str, List[str]]
    lineage: Dict[str, List[Dict[str, str]]]
    governance: Dict[str, Any]
    tags: List[str]
    description: str
    created_at: str
    created_by: str


class UnityCatalogClient:
    """Client for Unity Catalog REST API"""

    def __init__(self, base_url: str, token: Optional[str] = None):
        self.base_url = base_url.rstrip('/')
        self.token = token
        self.session = requests.Session()

        if token:
            self.session.headers.update({
                'Authorization': f'Bearer {token}'
            })

    def _get(self, endpoint: str, params: Optional[Dict] = None) -> Dict:
        """Make GET request to Unity Catalog API"""
        url = f"{self.base_url}{endpoint}"
        response = self.session.get(url, params=params)
        response.raise_for_status()
        return response.json()

    def list_catalogs(self) -> List[str]:
        """List all catalogs"""
        response = self._get('/api/2.1/unity-catalog/catalogs')
        return [cat['name'] for cat in response.get('catalogs', [])]

    def list_schemas(self, catalog_name: str) -> List[str]:
        """List all schemas in a catalog"""
        response = self._get(
            '/api/2.1/unity-catalog/schemas',
            params={'catalog_name': catalog_name}
        )
        return [sch['name'] for sch in response.get('schemas', [])]

    def list_tables(self, catalog_name: str, schema_name: str) -> List[str]:
        """List all tables in a schema"""
        response = self._get(
            '/api/2.1/unity-catalog/tables',
            params={
                'catalog_name': catalog_name,
                'schema_name': schema_name
            }
        )
        return [tbl['name'] for tbl in response.get('tables', [])]

    def get_table(self, full_name: str) -> TableInfo:
        """Get table details including schema"""
        # full_name format: catalog.schema.table
        response = self._get(f'/api/2.1/unity-catalog/tables/{full_name}')

        return TableInfo(
            catalog_name=response['catalog_name'],
            schema_name=response['schema_name'],
            table_name=response['name'],
            table_type=response.get('table_type', 'MANAGED'),
            data_source_format=response.get('data_source_format', 'DELTA'),
            storage_location=response.get('storage_location', ''),
            owner=response.get('owner', ''),
            created_at=response.get('created_at', ''),
            updated_at=response.get('updated_at', ''),
            columns=response.get('columns', []),
            properties=response.get('properties', {})
        )

    def get_table_lineage(self, full_name: str) -> Dict:
        """Get table lineage information"""
        try:
            response = self._get(
                '/api/2.1/unity-catalog/lineage/table-lineage',
                params={'table_name': full_name}
            )
            return response
        except requests.exceptions.HTTPError as e:
            # Lineage may not be available for all tables
            if e.response.status_code == 404:
                return {'upstreams': [], 'downstreams': []}
            raise


class IcebergToAvroConverter:
    """Convert Iceberg schemas to Avro format"""

    TYPE_MAPPING = {
        'boolean': 'boolean',
        'int': 'int',
        'long': 'long',
        'float': 'float',
        'double': 'double',
        'string': 'string',
        'binary': 'bytes',
        'date': {'type': 'int', 'logicalType': 'date'},
        'timestamp': {'type': 'long', 'logicalType': 'timestamp-micros'},
        'timestamptz': {'type': 'long', 'logicalType': 'timestamp-micros'},
    }

    def convert_column_type(self, column: Dict) -> Any:
        """Convert Unity Catalog column type to Avro type"""
        col_type = column['type_name']
        nullable = column.get('nullable', True)

        # Handle complex types
        if col_type.startswith('decimal'):
            # decimal(precision, scale)
            import re
            match = re.match(r'decimal\((\d+),(\d+)\)', col_type)
            if match:
                precision, scale = int(match.group(1)), int(match.group(2))
                avro_type = {
                    'type': 'bytes',
                    'logicalType': 'decimal',
                    'precision': precision,
                    'scale': scale
                }
            else:
                avro_type = 'bytes'

        elif col_type.startswith('array'):
            # array<element_type>
            element_type = col_type[6:-1]  # Extract element type
            avro_type = {
                'type': 'array',
                'items': self._simple_type_mapping(element_type)
            }

        elif col_type.startswith('map'):
            # map<key_type, value_type>
            import re
            match = re.match(r'map<(.+),(.+)>', col_type)
            if match:
                key_type = match.group(1).strip()
                value_type = match.group(2).strip()
                avro_type = {
                    'type': 'map',
                    'values': self._simple_type_mapping(value_type)
                }
            else:
                avro_type = {'type': 'map', 'values': 'string'}

        elif col_type.startswith('struct'):
            # struct<field1:type1,field2:type2,...>
            # Simplified - would need proper parsing for production
            avro_type = {
                'type': 'record',
                'name': column['name'] + '_struct',
                'fields': []  # Would parse struct fields here
            }

        else:
            # Simple type
            avro_type = self._simple_type_mapping(col_type)

        # Handle nullability
        if nullable and avro_type != 'null':
            return ['null', avro_type]
        else:
            return avro_type

    def _simple_type_mapping(self, type_name: str) -> Any:
        """Map simple Unity Catalog types to Avro"""
        return self.TYPE_MAPPING.get(type_name.lower(), 'string')

    def convert_table_to_avro(self, table_info: TableInfo) -> Dict:
        """Convert Unity Catalog table to Avro schema"""
        namespace = f"{table_info.catalog_name}.{table_info.schema_name}"
        name = table_info.table_name

        fields = []
        for column in table_info.columns:
            field = {
                'name': column['name'],
                'type': self.convert_column_type(column),
                'doc': column.get('comment', f"Imported from Unity Catalog")
            }

            # Add default value for nullable fields
            if isinstance(field['type'], list) and 'null' in field['type']:
                field['default'] = None

            fields.append(field)

        avro_schema = {
            'type': 'record',
            'name': name,
            'namespace': namespace,
            'doc': f"Imported from Unity Catalog: {table_info.catalog_name}.{table_info.schema_name}.{table_info.table_name}",
            'fields': fields
        }

        return avro_schema


class UnityCatalogImporter:
    """Main importer class for Unity Catalog schemas"""

    def __init__(self,
                 uc_client: UnityCatalogClient,
                 schema_id_service_url: str,
                 schemastore_path: str):
        self.uc_client = uc_client
        self.schema_id_service_url = schema_id_service_url
        self.schemastore_path = schemastore_path
        self.converter = IcebergToAvroConverter()

    def allocate_schema_id(self, schema_content: str, subject: str, version: int) -> int:
        """Allocate global schema ID"""
        url = f"{self.schema_id_service_url}/api/v1/schema-ids/allocate"

        payload = {
            'subject': subject,
            'version': version,
            'context': 'unity-catalog',
            'schemaType': 'ICEBERG',
            'schemaContent': schema_content
        }

        response = requests.post(url, json=payload)
        response.raise_for_status()

        result = response.json()
        return result['id']

    def import_table(self,
                     catalog: str,
                     schema: str,
                     table: str,
                     version: int = 1,
                     deployment_targets: Optional[List[str]] = None) -> Dict:
        """
        Import a single table from Unity Catalog

        Returns:
            Dict with schema_id, avro_schema, metadata
        """
        # Get table information from Unity Catalog
        full_name = f"{catalog}.{schema}.{table}"
        table_info = self.uc_client.get_table(full_name)

        # Convert to Avro schema
        avro_schema = self.converter.convert_table_to_avro(table_info)
        schema_json = json.dumps(avro_schema, indent=2)

        # Allocate global schema ID
        subject = f"{catalog}.{schema}.{table}"
        schema_id = self.allocate_schema_id(schema_json, subject, version)

        # Get lineage information
        lineage = self.uc_client.get_table_lineage(full_name)

        # Create metadata
        metadata = SchemaMetadata(
            schema_id=schema_id,
            subject=subject,
            version=version,
            context='unity-catalog',
            schema_type='ICEBERG',
            platform='unity-catalog',
            source_info={
                'platform': 'unity-catalog',
                'catalog': catalog,
                'schema': schema,
                'table': table,
                'tableFormat': table_info.data_source_format,
                'location': table_info.storage_location,
                'provider': 'UNITY_CATALOG',
                'importedAt': datetime.utcnow().isoformat() + 'Z',
                'lastModified': table_info.updated_at
            },
            compatibility_mode='BACKWARD',
            deployment_targets={
                'kafkaTopics': [],
                'unityTables': [full_name],
                'snowflakeTables': []
            },
            lineage={
                'upstreamSources': [
                    {
                        'type': 'unity-table',
                        'name': up['table_name']
                    }
                    for up in lineage.get('upstreams', [])
                ],
                'downstreamTargets': [
                    {
                        'type': 'unity-table',
                        'name': down['table_name']
                    }
                    for down in lineage.get('downstreams', [])
                ],
                'transformations': []
            },
            governance={
                'owner': table_info.owner,
                'classification': 'UNCLASSIFIED',
                'piiFields': [],
                'retentionDays': 0,
                'approvedBy': '',
                'approvalDate': ''
            },
            tags=['unity-catalog', 'iceberg', 'imported'],
            description=f"Imported from Unity Catalog: {full_name}",
            created_at=datetime.utcnow().isoformat() + 'Z',
            created_by='unity-catalog-importer'
        )

        # Save to SCHEMASTORE
        self._save_to_schemastore(
            catalog=catalog,
            schema=schema,
            table=table,
            version=version,
            avro_schema=avro_schema,
            metadata=metadata,
            original_columns=table_info.columns
        )

        return {
            'schema_id': schema_id,
            'subject': subject,
            'avro_schema': avro_schema,
            'metadata': asdict(metadata)
        }

    def _save_to_schemastore(self,
                             catalog: str,
                             schema: str,
                             table: str,
                             version: int,
                             avro_schema: Dict,
                             metadata: SchemaMetadata,
                             original_columns: List[Dict]):
        """Save imported schema to SCHEMASTORE"""
        import os

        # Create directory structure
        schema_dir = os.path.join(
            self.schemastore_path,
            'unity-catalog',
            catalog,
            schema,
            table,
            f'v{version}'
        )
        os.makedirs(schema_dir, exist_ok=True)

        # Write Avro schema
        avro_file = os.path.join(schema_dir, 'schema.avsc')
        with open(avro_file, 'w') as f:
            json.dump(avro_schema, f, indent=2)

        # Write original Iceberg schema (Unity Catalog columns)
        iceberg_file = os.path.join(schema_dir, 'schema.iceberg.json')
        with open(iceberg_file, 'w') as f:
            json.dump({'columns': original_columns}, f, indent=2)

        # Write metadata
        metadata_file = os.path.join(schema_dir, 'metadata.json')
        with open(metadata_file, 'w') as f:
            json.dump(asdict(metadata), f, indent=2)

        print(f"✓ Saved schema to {schema_dir}")

    def import_all_tables(self, catalog: str, schema: str) -> List[Dict]:
        """Import all tables from a schema"""
        tables = self.uc_client.list_tables(catalog, schema)
        results = []

        for table in tables:
            try:
                result = self.import_table(catalog, schema, table)
                results.append(result)
                print(f"✓ Imported {catalog}.{schema}.{table} → Schema ID {result['schema_id']}")
            except Exception as e:
                print(f"✗ Failed to import {catalog}.{schema}.{table}: {e}")
                results.append({
                    'table': f"{catalog}.{schema}.{table}",
                    'error': str(e)
                })

        return results

    def discover_all_schemas(self) -> Dict[str, List[str]]:
        """Discover all catalogs, schemas, and tables"""
        discovery = {}

        catalogs = self.uc_client.list_catalogs()
        for catalog in catalogs:
            discovery[catalog] = {}
            schemas = self.uc_client.list_schemas(catalog)

            for schema in schemas:
                tables = self.uc_client.list_tables(catalog, schema)
                discovery[catalog][schema] = tables

        return discovery


# ============================================================================
# CLI Interface
# ============================================================================

def main():
    """CLI for Unity Catalog importer"""
    import argparse

    parser = argparse.ArgumentParser(
        description='Import schemas from Unity Catalog to GitOps Schema Store'
    )
    parser.add_argument('--uc-url', required=True, help='Unity Catalog URL')
    parser.add_argument('--uc-token', help='Unity Catalog auth token')
    parser.add_argument('--schema-id-service', required=True,
                        help='Schema ID service URL')
    parser.add_argument('--schemastore-path', required=True,
                        help='Path to SCHEMASTORE directory')

    subparsers = parser.add_subparsers(dest='command', help='Commands')

    # Discover command
    parser_discover = subparsers.add_parser('discover',
                                             help='Discover all tables')

    # Import table command
    parser_import = subparsers.add_parser('import-table',
                                           help='Import single table')
    parser_import.add_argument('--catalog', required=True)
    parser_import.add_argument('--schema', required=True)
    parser_import.add_argument('--table', required=True)
    parser_import.add_argument('--version', type=int, default=1)

    # Import schema command
    parser_import_schema = subparsers.add_parser('import-schema',
                                                  help='Import all tables in schema')
    parser_import_schema.add_argument('--catalog', required=True)
    parser_import_schema.add_argument('--schema', required=True)

    args = parser.parse_args()

    # Initialize client and importer
    uc_client = UnityCatalogClient(args.uc_url, args.uc_token)
    importer = UnityCatalogImporter(
        uc_client=uc_client,
        schema_id_service_url=args.schema_id_service,
        schemastore_path=args.schemastore_path
    )

    # Execute command
    if args.command == 'discover':
        discovery = importer.discover_all_schemas()
        print(json.dumps(discovery, indent=2))

    elif args.command == 'import-table':
        result = importer.import_table(
            catalog=args.catalog,
            schema=args.schema,
            table=args.table,
            version=args.version
        )
        print(f"✓ Imported schema ID: {result['schema_id']}")

    elif args.command == 'import-schema':
        results = importer.import_all_tables(args.catalog, args.schema)
        print(f"\nImported {len(results)} tables")

    else:
        parser.print_help()


if __name__ == '__main__':
    main()
