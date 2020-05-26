package io.confluent.kafka.schemaregistry.metrics;

import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.utils.TestUtils;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class MetricsTest extends ClusterTestHarness {

  private MetricsContainer container;

  public MetricsTest() { super(1, true); }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    container = restApp.restApp.schemaRegistry().getMetricsContainer();
  }

  @Test
  public void testSchemaCreatedCount() throws Exception {
    RestService service = restApp.restClient;

    String subject = "testTopic1";
    int schemaCount = 3;
    List<String> schemas = TestUtils.getRandomCanonicalAvroString(schemaCount);

    // test registering and verifying new schemas in subject1
    int schemaIdCounter = 1;
    for (int i = 0; i < schemaCount; i++) {
      String schema = schemas.get(i);
      TestUtils.registerAndVerifySchema(service, schema, schemaIdCounter++, subject);
    }
    assertEquals(schemaCount, container.getSchemasCreated().get());
    assertEquals(schemaCount, container.getSchemasCreated(AvroSchema.TYPE).get());

    // Re-registering schemas should not increase metrics.
    for (int i = 0; i < schemaCount; i++) {
      String schemaString = schemas.get(i);
      service.registerSchema(schemaString, subject);
    }

    assertEquals(schemaCount, container.getSchemasCreated().get());
    assertEquals(schemaCount, container.getSchemasCreated(AvroSchema.TYPE).get());

    for (Integer i = 1; i < schemaIdCounter; i++) {
      assertEquals(i, service.deleteSchemaVersion(RestService.DEFAULT_REQUEST_PROPERTIES,
                                                  subject, i.toString()));
    }

    // Deleting schemas should not modify create count.
    assertEquals(schemaCount, container.getSchemasCreated().get());
    assertEquals(schemaCount, container.getSchemasCreated(AvroSchema.TYPE).get());

    assertEquals(schemaCount, container.getSchemasDeleted().get());
    assertEquals(schemaCount, container.getSchemasDeleted(AvroSchema.TYPE).get());
  }

  @Test
  public void testApiCallMetrics() throws Exception {
    String subject1 = "testTopic1";
    int schemaCount = 3;
    List<String> schemas = TestUtils.getRandomCanonicalAvroString(schemaCount);

    // test registering and verifying new schemas in subject1
    int schemaIdCounter = 1;
    for (int i = 0; i < schemaCount; i++) {
      String schema = schemas.get(i);
      TestUtils.registerAndVerifySchema(restApp.restClient, schema, schemaIdCounter++, subject1);
    }

    // We perform two operations (register & get) for each schema
    assertEquals(schemaCount * 2, container.getApiCallsSuccess().get());
    assertEquals(0, container.getApiCallsFailure().get());

    try {
      restApp.restClient.getId(100);
      fail("Schema lookup with missing ID expected to fail");
    } catch (RestClientException rce) {
      assertEquals(1, container.getApiCallsFailure().get());
    }
  }
}
