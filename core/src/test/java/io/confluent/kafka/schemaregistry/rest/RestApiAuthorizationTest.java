package io.confluent.kafka.schemaregistry.rest;

import io.confluent.kafka.schemaregistry.SSLClusterRequiringAuthorizationTestHarness;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class RestApiAuthorizationTest extends SSLClusterRequiringAuthorizationTestHarness {

    public static final String TEST_TOPIC = "some-topic";
    public static final String TEST_USER = "test-client";
    public static final String TEST_SCHEMA = "{\"type\": \"string\"}";
    public static final String TEST_SCHEMA_BODY = "{\"schema\": \"{\\\"type\\\": \\\"string\\\"}\"}";
    public static final String EXPECTED_200_MSG = "Response status must be 200.";
    public static final String EXPECTED_401_MSG = "Response status must be 401.";

    public RestApiAuthorizationTest() {
        super(TEST_USER);
    }

    @Test
    public void testAuthorizationWithCorrectCertificateAndACLsUsingHttpClient() throws Exception {
        addWriteAcl(TEST_USER, TEST_TOPIC);
        int statusCode = updateSchemaUsingHttpClient(TEST_TOPIC, TEST_SCHEMA_BODY);
        assertEquals(EXPECTED_200_MSG, 200, statusCode);
    }

    @Test
    public void testAuthorizationWithCorrectCertificateButLackOfACLsUsingHttpClient() throws Exception {
        int statusCode = updateSchemaUsingHttpClient(TEST_TOPIC, TEST_SCHEMA_BODY);
        assertEquals(EXPECTED_401_MSG, 401, statusCode);
    }

    @Test
    public void testAuthorizationWithCorrectCertificateAndACLsUsingSchemaRegistryClient() throws Exception {
        addWriteAcl(TEST_USER, TEST_TOPIC);
        updateSchemaRegistryUsingSchemaRegistryClient(TEST_TOPIC, TEST_SCHEMA);
    }

    @Test(expected = RestClientException.class)
    public void testAuthorizationWithCorrectCertificateButLackOfACLsUsingSchemaRegistryClient() throws Exception {
        updateSchemaRegistryUsingSchemaRegistryClient(TEST_TOPIC, TEST_SCHEMA);
    }
}
