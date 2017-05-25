package io.confluent.kafka.schemaregistry.rest;

import io.confluent.kafka.schemaregistry.SSLClusterRequiringAuthorizationTestHarness;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class RestApiAuthorizationTest extends SSLClusterRequiringAuthorizationTestHarness {

    public static final String TEST_TOPIC = "some-topic";
    public static final String TEST_USER = "test-client";
    public static final String TEST_SCHEMA = "{\"schema\": \"{\\\"type\\\": \\\"string\\\"}\"}";
    public static final String EXPECTED_200_MSG = "Response status must be 200.";
    public static final String EXPECTED_401_MSG = "Response status must be 401.";

    public RestApiAuthorizationTest() {
        super(TEST_USER);
    }

    @Test
    public void testAuthorizationWithCorrectCertificateAndACLs() throws Exception {
        addWriteAcl(TEST_USER, TEST_TOPIC);
        int statusCode = updateSchema(TEST_TOPIC, TEST_SCHEMA);
        assertEquals(EXPECTED_200_MSG, 200, statusCode);
    }

    @Test
    public void testAuthorizationWithCorrectCertificateButLackOfACLs() throws Exception {
        int statusCode = updateSchema(TEST_TOPIC, TEST_SCHEMA);
        assertEquals(EXPECTED_401_MSG, 401, statusCode);
    }
}
