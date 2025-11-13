"""
Integration tests for Unity Catalog schema import

Tests the complete flow:
1. Connect to Unity Catalog
2. Discover tables
3. Import schemas
4. Verify SCHEMASTORE structure
5. Validate metadata
"""

import pytest
import json
import os
import tempfile
import shutil
from pathlib import Path
import sys

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from importers.unity_catalog_importer import (
    UnityCatalogClient,
    IcebergToAvroConverter,
    UnityCatalogImporter,
    TableInfo
)


@pytest.fixture
def uc_url():
    """Unity Catalog URL from environment or default"""
    return os.getenv('UC_URL', 'http://localhost:8080')


@pytest.fixture
def schema_id_service_url():
    """Schema ID service URL"""
    return os.getenv('SCHEMA_ID_SERVICE_URL', 'http://localhost:8090')


@pytest.fixture
def temp_schemastore():
    """Temporary SCHEMASTORE directory"""
    tmpdir = tempfile.mkdtemp(prefix='schemastore_test_')
    yield tmpdir
    shutil.rmtree(tmpdir)


@pytest.fixture
def uc_client(uc_url):
    """Unity Catalog client instance"""
    return UnityCatalogClient(uc_url)


@pytest.fixture
def importer(uc_client, schema_id_service_url, temp_schemastore):
    """Unity Catalog importer instance"""
    return UnityCatalogImporter(
        uc_client=uc_client,
        schema_id_service_url=schema_id_service_url,
        schemastore_path=temp_schemastore
    )


class TestUnityCatalogConnection:
    """Test Unity Catalog connectivity"""

    def test_list_catalogs(self, uc_client):
        """Test listing catalogs"""
        catalogs = uc_client.list_catalogs()
        assert isinstance(catalogs, list)
        assert 'main' in catalogs, "Demo catalog 'main' should exist"

    def test_list_schemas(self, uc_client):
        """Test listing schemas in catalog"""
        schemas = uc_client.list_schemas('main')
        assert isinstance(schemas, list)
        assert 'bronze' in schemas, "Demo schema 'bronze' should exist"

    def test_list_tables(self, uc_client):
        """Test listing tables in schema"""
        tables = uc_client.list_tables('main', 'bronze')
        assert isinstance(tables, list)
        assert len(tables) >= 2, "Should have at least users and orders tables"
        assert 'users' in tables
        assert 'orders' in tables

    def test_get_table_details(self, uc_client):
        """Test getting table details"""
        table_info = uc_client.get_table('main.bronze.users')

        assert isinstance(table_info, TableInfo)
        assert table_info.catalog_name == 'main'
        assert table_info.schema_name == 'bronze'
        assert table_info.table_name == 'users'
        assert len(table_info.columns) > 0

        # Verify key columns exist
        column_names = [col['name'] for col in table_info.columns]
        assert 'user_id' in column_names
        assert 'email' in column_names


class TestIcebergToAvroConversion:
    """Test Iceberg schema to Avro conversion"""

    @pytest.fixture
    def converter(self):
        return IcebergToAvroConverter()

    @pytest.fixture
    def sample_table_info(self):
        """Sample Unity Catalog table info"""
        return TableInfo(
            catalog_name='main',
            schema_name='bronze',
            table_name='users',
            table_type='MANAGED',
            data_source_format='DELTA',
            storage_location='s3://warehouse/main.db/users',
            owner='demo',
            created_at='2025-11-13T10:00:00Z',
            updated_at='2025-11-13T10:00:00Z',
            columns=[
                {
                    'name': 'user_id',
                    'type_name': 'string',
                    'nullable': False,
                    'comment': 'User identifier'
                },
                {
                    'name': 'email',
                    'type_name': 'string',
                    'nullable': True,
                    'comment': 'Email address'
                },
                {
                    'name': 'age',
                    'type_name': 'int',
                    'nullable': True,
                    'comment': 'User age'
                },
                {
                    'name': 'created_at',
                    'type_name': 'timestamp',
                    'nullable': False,
                    'comment': 'Creation timestamp'
                },
                {
                    'name': 'balance',
                    'type_name': 'decimal(10,2)',
                    'nullable': True,
                    'comment': 'Account balance'
                },
                {
                    'name': 'tags',
                    'type_name': 'array<string>',
                    'nullable': True,
                    'comment': 'User tags'
                }
            ],
            properties={}
        )

    def test_convert_simple_types(self, converter):
        """Test conversion of simple types"""
        # String (non-nullable)
        col = {'name': 'user_id', 'type_name': 'string', 'nullable': False}
        avro_type = converter.convert_column_type(col)
        assert avro_type == 'string'

        # String (nullable)
        col = {'name': 'email', 'type_name': 'string', 'nullable': True}
        avro_type = converter.convert_column_type(col)
        assert avro_type == ['null', 'string']

        # Integer
        col = {'name': 'age', 'type_name': 'int', 'nullable': False}
        avro_type = converter.convert_column_type(col)
        assert avro_type == 'int'

        # Timestamp
        col = {'name': 'ts', 'type_name': 'timestamp', 'nullable': False}
        avro_type = converter.convert_column_type(col)
        assert isinstance(avro_type, dict)
        assert avro_type['type'] == 'long'
        assert avro_type['logicalType'] == 'timestamp-micros'

    def test_convert_decimal_type(self, converter):
        """Test decimal type conversion"""
        col = {'name': 'balance', 'type_name': 'decimal(10,2)', 'nullable': False}
        avro_type = converter.convert_column_type(col)

        assert isinstance(avro_type, dict)
        assert avro_type['type'] == 'bytes'
        assert avro_type['logicalType'] == 'decimal'
        assert avro_type['precision'] == 10
        assert avro_type['scale'] == 2

    def test_convert_array_type(self, converter):
        """Test array type conversion"""
        col = {'name': 'tags', 'type_name': 'array<string>', 'nullable': False}
        avro_type = converter.convert_column_type(col)

        assert isinstance(avro_type, dict)
        assert avro_type['type'] == 'array'
        assert avro_type['items'] == 'string'

    def test_convert_table_to_avro(self, converter, sample_table_info):
        """Test full table conversion"""
        avro_schema = converter.convert_table_to_avro(sample_table_info)

        # Verify schema structure
        assert avro_schema['type'] == 'record'
        assert avro_schema['name'] == 'users'
        assert avro_schema['namespace'] == 'main.bronze'
        assert len(avro_schema['fields']) == 6

        # Verify specific fields
        fields_by_name = {f['name']: f for f in avro_schema['fields']}

        # user_id (non-nullable string)
        assert fields_by_name['user_id']['type'] == 'string'

        # email (nullable string)
        assert fields_by_name['email']['type'] == ['null', 'string']
        assert fields_by_name['email']['default'] is None

        # created_at (timestamp)
        ts_type = fields_by_name['created_at']['type']
        assert ts_type['logicalType'] == 'timestamp-micros'


class TestSchemaImport:
    """Test schema import functionality"""

    def test_import_single_table(self, importer, temp_schemastore):
        """Test importing a single table"""
        result = importer.import_table(
            catalog='main',
            schema='bronze',
            table='users',
            version=1
        )

        # Verify result structure
        assert 'schema_id' in result
        assert 'subject' in result
        assert 'avro_schema' in result
        assert 'metadata' in result

        # Verify schema ID allocated
        assert result['schema_id'] > 0

        # Verify subject format
        assert result['subject'] == 'main.bronze.users'

        # Verify Avro schema
        avro_schema = result['avro_schema']
        assert avro_schema['type'] == 'record'
        assert avro_schema['name'] == 'users'

        # Verify metadata
        metadata = result['metadata']
        assert metadata['context'] == 'unity-catalog'
        assert metadata['platform'] == 'unity-catalog'
        assert metadata['source_info']['catalog'] == 'main'
        assert metadata['source_info']['schema'] == 'bronze'
        assert metadata['source_info']['table'] == 'users'

    def test_schemastore_structure(self, importer, temp_schemastore):
        """Test SCHEMASTORE directory structure"""
        importer.import_table('main', 'bronze', 'users', version=1)

        # Verify directory structure
        expected_dir = Path(temp_schemastore) / 'unity-catalog' / 'main' / 'bronze' / 'users' / 'v1'
        assert expected_dir.exists()
        assert expected_dir.is_dir()

        # Verify files exist
        assert (expected_dir / 'schema.avsc').exists()
        assert (expected_dir / 'schema.iceberg.json').exists()
        assert (expected_dir / 'metadata.json').exists()

        # Verify Avro schema is valid JSON
        with open(expected_dir / 'schema.avsc') as f:
            avro_schema = json.load(f)
            assert avro_schema['type'] == 'record'

        # Verify metadata is valid JSON
        with open(expected_dir / 'metadata.json') as f:
            metadata = json.load(f)
            assert metadata['schemaId'] > 0
            assert metadata['context'] == 'unity-catalog'

    def test_import_multiple_tables(self, importer):
        """Test importing all tables in a schema"""
        results = importer.import_all_tables('main', 'bronze')

        # Should have imported users and orders
        assert len(results) >= 2

        # All should be successful
        for result in results:
            assert 'error' not in result or result['error'] is None
            assert 'schema_id' in result

        # Verify unique schema IDs
        schema_ids = [r['schema_id'] for r in results if 'schema_id' in r]
        assert len(schema_ids) == len(set(schema_ids)), "Schema IDs should be unique"

    def test_import_idempotency(self, importer):
        """Test that re-importing same table returns same schema ID"""
        # Import once
        result1 = importer.import_table('main', 'bronze', 'users', version=1)
        schema_id_1 = result1['schema_id']

        # Import again (should be idempotent)
        result2 = importer.import_table('main', 'bronze', 'users', version=1)
        schema_id_2 = result2['schema_id']

        # Should return same schema ID
        assert schema_id_1 == schema_id_2


class TestSchemaDiscovery:
    """Test schema discovery functionality"""

    def test_discover_all_schemas(self, importer):
        """Test discovering all catalogs, schemas, and tables"""
        discovery = importer.discover_all_schemas()

        # Verify structure
        assert isinstance(discovery, dict)
        assert 'main' in discovery
        assert 'bronze' in discovery['main']
        assert isinstance(discovery['main']['bronze'], list)

        # Verify tables exist
        tables = discovery['main']['bronze']
        assert 'users' in tables
        assert 'orders' in tables


class TestLineageTracking:
    """Test lineage tracking (if available)"""

    def test_get_table_lineage(self, uc_client):
        """Test getting table lineage"""
        lineage = uc_client.get_table_lineage('main.bronze.users')

        # Lineage should be a dict with upstreams and downstreams
        assert isinstance(lineage, dict)
        assert 'upstreams' in lineage or 'downstreams' in lineage


@pytest.mark.integration
class TestEndToEndFlow:
    """End-to-end integration tests"""

    def test_complete_import_workflow(self, importer, temp_schemastore):
        """Test complete workflow: discover → import → verify"""
        # Step 1: Discover tables
        discovery = importer.discover_all_schemas()
        assert len(discovery) > 0

        # Step 2: Import a table
        result = importer.import_table('main', 'bronze', 'users', version=1)
        schema_id = result['schema_id']

        # Step 3: Verify SCHEMASTORE
        schema_dir = Path(temp_schemastore) / 'unity-catalog' / 'main' / 'bronze' / 'users' / 'v1'
        assert schema_dir.exists()

        # Step 4: Verify metadata correctness
        with open(schema_dir / 'metadata.json') as f:
            metadata = json.load(f)

        assert metadata['schemaId'] == schema_id
        assert metadata['subject'] == 'main.bronze.users'
        assert metadata['context'] == 'unity-catalog'
        assert metadata['platform'] == 'unity-catalog'
        assert 'source_info' in metadata
        assert metadata['source_info']['catalog'] == 'main'

        # Step 5: Verify Avro schema has all fields
        with open(schema_dir / 'schema.avsc') as f:
            avro_schema = json.load(f)

        field_names = [f['name'] for f in avro_schema['fields']]
        assert 'user_id' in field_names
        assert 'email' in field_names


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
