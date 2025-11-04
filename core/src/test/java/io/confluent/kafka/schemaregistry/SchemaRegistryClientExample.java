package io.confluent.kafka.schemaregistry;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Simple example program demonstrating Schema Registry Java client usage.
 *
 * This program:
 * 1. Creates a Schema Registry client with basic authentication
 * 2. Registers a schema (Avro or JSON) to a subject
 * 3. Looks up the subject to verify the schema was registered
 */
public class SchemaRegistryClientExample {

    // Schema type to use: "AVRO" or "JSON"
    private static final String SCHEMA_TYPE = "JSON";

    // Schema Registry API Endpoint
    private static final String SCHEMA_REGISTRY_URL = "https://localhost:6444";

    // Basic Auth Credentials
    private static final String API_KEY = "tenant1-key";
    private static final String API_SECRET = "nohash";

    // Subject name for the schema
    private static final String SUBJECT_NAME = "example-subject-67k-79-4";

    // Path to the schema file (Avro or JSON based on SCHEMA_TYPE)
    private static final String SCHEMA_FILE_PATH = "/Users/karthikeyansrinivasan/Work/PL4SR/PPV2/schema-67K.json";
    //private static final String SCHEMA_FILE_PATH = "/Users/karthikeyansrinivasan/Work/PL4SR/PPV2/schema-8K.json";

    public static void main(String[] args) {
        SchemaRegistryClient client = null;

        try {
            // Step 1: Read schema from file
            String schemaFilePath = (args.length > 0) ? args[0] : SCHEMA_FILE_PATH;
            String schemaString = readSchemaFromFile(schemaFilePath);
            System.out.println("✓ Schema file read successfully: " + schemaFilePath);

            // Step 2: Create Schema Registry client with basic auth
            client = createSchemaRegistryClient();
            System.out.println("✓ Schema Registry client created successfully");

            // Step 3: Parse the schema based on SCHEMA_TYPE
            ParsedSchema parsedSchema = parseSchema(schemaString, SCHEMA_TYPE);
            System.out.println("✓ " + SCHEMA_TYPE + " schema parsed successfully");

            // Step 4: Register the schema under a subject
            //int schemaId = registerSchema(client, SUBJECT_NAME, parsedSchema);
            //System.out.println("✓ Schema registered successfully with ID: " + schemaId);

            // Step 5: Lookup the subject to verify registration
            System.out.println("\n=== Performing Schema Lookup ===");
            lookupSubject(client, SUBJECT_NAME, parsedSchema);

            // Step 6: Get schema metadata for the subject
            //System.out.println("\n=== Get Schema Metadata ===");
            //getSchemaMetadata(client, SUBJECT_NAME);

            System.out.println("\n=== All operations completed successfully! ===");

        } catch (IOException | RestClientException e) {
            System.err.println("Error occurred: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // Clean up resources
            if (client != null) {
                try {
                    client.close();
                    System.out.println("\n✓ Client closed successfully");
                } catch (IOException e) {
                    System.err.println("Error closing client: " + e.getMessage());
                }
            }
        }
    }

    /**
     * Reads schema content from a file.
     *
     * @param filePath Path to the schema file
     * @return Schema content as a string
     * @throws IOException if file cannot be read
     */
    private static String readSchemaFromFile(String filePath) throws IOException {
        Path path = Paths.get(filePath);

        if (!Files.exists(path)) {
            throw new IOException("Schema file not found: " + filePath);
        }

        // Read all bytes from the file and convert to string
        byte[] bytes = Files.readAllBytes(path);
        String schemaContent = new String(bytes, "UTF-8").trim();

        if (schemaContent.isEmpty()) {
            throw new IOException("Schema file is empty: " + filePath);
        }

        return schemaContent;
    }

    /**
     * Parses the schema string based on the schema type.
     *
     * @param schemaString Schema content as a string
     * @param schemaType Type of schema: "AVRO" or "JSON"
     * @return ParsedSchema instance
     * @throws IllegalArgumentException if schema type is not supported
     */
    private static ParsedSchema parseSchema(String schemaString, String schemaType) {
        switch (schemaType.toUpperCase()) {
            case "AVRO":
                return new AvroSchema(schemaString);
            case "JSON":
                return new JsonSchema(schemaString);
            default:
                throw new IllegalArgumentException(
                    "Unsupported schema type: " + schemaType + ". Supported types: AVRO, JSON");
        }
    }

    /**
     * Creates a Schema Registry client with basic authentication configured.
     *
     * @return SchemaRegistryClient instance
     */
    private static SchemaRegistryClient createSchemaRegistryClient() {
        // Configure basic authentication
        Map<String, String> config = new HashMap<>();
        config.put(SchemaRegistryClientConfig.BASIC_AUTH_CREDENTIALS_SOURCE, "USER_INFO");
        config.put(SchemaRegistryClientConfig.USER_INFO_CONFIG, API_KEY + ":" + API_SECRET);

        // Optional: Configure HTTP timeouts
        config.put(SchemaRegistryClientConfig.HTTP_CONNECT_TIMEOUT_MS, "30000");
        config.put(SchemaRegistryClientConfig.HTTP_READ_TIMEOUT_MS, "30000");

        // Create client with configuration
        // Parameters: URL, cache capacity, config map, http headers
        return new CachedSchemaRegistryClient(
            Collections.singletonList(SCHEMA_REGISTRY_URL),
            100,  // cache capacity
            config,
            null  // no additional HTTP headers
        );
    }

    /**
     * Registers a schema under the specified subject.
     *
     * @param client SchemaRegistryClient instance
     * @param subject Subject name
     * @param schema ParsedSchema to register
     * @return Schema ID assigned by Schema Registry
     * @throws IOException if I/O error occurs
     * @throws RestClientException if REST API error occurs
     */
    private static int registerSchema(SchemaRegistryClient client, String subject,
                                      ParsedSchema schema)
            throws IOException, RestClientException {
        int schemaId = client.register(subject, schema);
        return schemaId;
    }

    /**
     * Looks up a subject with the given schema (equivalent to POST /subjects/{subject}).
     * This returns the schema ID if the schema already exists for the subject.
     *
     * @param client SchemaRegistryClient instance
     * @param subject Subject name
     * @param schema ParsedSchema to lookup
     * @throws IOException if I/O error occurs
     * @throws RestClientException if REST API error occurs
     */
    private static void lookupSubject(SchemaRegistryClient client, String subject,
                                      ParsedSchema schema)
            throws IOException, RestClientException {
        // Get schema ID by subject and schema (POST /subjects/{subject})
        int schemaId = client.getId(subject, schema);
        System.out.println("✓ Schema lookup successful - Schema ID: " + schemaId);

        // Get version for the schema
        int version = client.getVersion(subject, schema);
        System.out.println("✓ Schema version: " + version);
    }

    /**
     * Retrieves schema metadata for the latest version of a subject.
     *
     * @param client SchemaRegistryClient instance
     * @param subject Subject name
     * @throws IOException if I/O error occurs
     * @throws RestClientException if REST API error occurs
     */
    private static void getSchemaMetadata(SchemaRegistryClient client, String subject)
            throws IOException, RestClientException {
        SchemaMetadata metadata = client.getLatestSchemaMetadata(subject);

        System.out.println("\n=== Schema Metadata ===");
        System.out.println("Subject: " + subject);
        System.out.println("Schema ID: " + metadata.getId());
        System.out.println("Version: " + metadata.getVersion());
        System.out.println("Schema Type: " + metadata.getSchemaType());
        System.out.println("Schema:\n" + metadata.getSchema());
    }
}
