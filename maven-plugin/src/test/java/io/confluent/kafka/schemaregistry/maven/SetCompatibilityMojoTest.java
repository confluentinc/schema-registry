package io.confluent.kafka.schemaregistry.maven;

import io.confluent.kafka.schemaregistry.CompatibilityLevel;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.apache.maven.plugin.MojoExecutionException;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertThrows;

public class SetCompatibilityMojoTest extends SchemaRegistryTest{
	SetCompatibilityMojo mojo;

	@Before
	public void createMojoAndFiles() {
		this.mojo = new SetCompatibilityMojo();
		this.mojo.client(new MockSchemaRegistryClient());
	}

	@Test
	public void specificSubjects() throws IOException, RestClientException, MojoExecutionException {
		String keySubject = String.format("TestSubject-key");
		Schema keySchema = Schema.create(Schema.Type.STRING);

		this.mojo.client().register(keySubject, new AvroSchema(keySchema));
		// Compatibility not set till now and hence should throw error
		assertThrows("Checking that compatibility hasn't been set",
				RestClientException.class, () -> this.mojo.client().getCompatibility(keySubject));

		// Setting compatibility & checking if it matches
		this.mojo.compatibilityLevels.put(keySubject,"BACKWARD");
		this.mojo.execute();

		assert(this.mojo.getConfig(keySubject).equals("BACKWARD"));

		//Updating to a different compatibility
		this.mojo.compatibilityLevels.replace(keySubject, "BACKWARD", "FULL");
		this.mojo.execute();

		assert(this.mojo.getConfig(keySubject).equals("FULL"));

		//Checking for Global Compatibility
		this.mojo.compatibilityLevels.put("__GLOBAL", "BACKWARD_TRANSITIVE");
		this.mojo.execute();
		assert(this.mojo.getConfig(null).equals("BACKWARD_TRANSITIVE"));

	}
}
