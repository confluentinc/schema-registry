/*
 * Copyright 2022 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
