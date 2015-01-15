/**
 * Copyright 2015 Confluent Inc.
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
 **/

package io.confluent.kafka.schemaregistry.tools;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Random;

import io.confluent.common.utils.AbstractPerformanceTest;
import io.confluent.common.utils.PerformanceStats;
import io.confluent.kafka.schemaregistry.avro.AvroCompatibilityLevel;
import io.confluent.kafka.schemaregistry.avro.AvroUtils;
import io.confluent.kafka.schemaregistry.rest.entities.requests.ConfigUpdateRequest;
import io.confluent.kafka.schemaregistry.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.utils.RestUtils;

public class SchemaRegistryPerformance extends AbstractPerformanceTest {
    long targetRegisteredSchemas;
    long targetSchemasPerSec;
    String baseUrl;
    String subject;
    long registeredSchemas = 0;

    public static void main(String[] args) throws Exception {

//        if (args.length < 1) {
//            System.out.println("Usage: java " + SchemaRegistryPerformance.class.getName() + "url");
//        }

        // /opt/kafka/bin/kafka-run-class.sh org.apache.kafka.clients.tools.ProducerPerformance test-rep-one 50000000 100 -1 bootstrap.servers=worker2:9092

        String baseUrl = "http://localhost:8080";
        String subject = "testSubject";
        int numSchemas = 5000;
        int targetSchemasPerSec = 1000;




//        if (args.length < 4) {
//            System.out.println(
//                    "Usage: java " + SchemaRegistryPerformance.class.getName() + " schema_registry_url"
//                    + " subject_name num_records target_schemas_per_sec"
//            );
//            System.exit(1);
//        }
//
//        String baseUrl = args[0];
//        String topic = args[1];
//        int numRecords = Integer.parseInt(args[2]);
//        int throughput = Integer.parseInt(args[3]);

        SchemaRegistryPerformance perf = new SchemaRegistryPerformance(baseUrl, subject, numSchemas, targetSchemasPerSec);

        // We need an approximate # of iterations per second, but we don't know how many records per request we'll receive
        // so we don't know how many iterations per second we need to hit the target rate. Get an approximate value using
        // the default max # of records per request the server will return.

        perf.run(targetSchemasPerSec); // target iterations per second
        perf.close();
    }

    public SchemaRegistryPerformance(String baseUrl, String subject, long numSchemas,
                                     long targetSchemasPerSec) throws Exception {
        super(numSchemas);
        this.baseUrl = baseUrl;
        this.subject = subject;
        this.targetRegisteredSchemas = numSchemas;
        this.targetSchemasPerSec = targetSchemasPerSec;

        // No compatibility verification
        ConfigUpdateRequest request = new ConfigUpdateRequest();
        request.setCompatibilityLevel(AvroCompatibilityLevel.NONE);
        RestUtils.updateConfig(this.baseUrl, RestUtils.DEFAULT_REQUEST_PROPERTIES, request, null);

        String groupId = "schema-registry-perf-" + Integer.toString(new Random().nextInt(100000));
    }

    // sequential schema maker
    private static String makeSchema(long num) {
        String schemaString = "{\"type\":\"record\","
                              + "\"name\":\"myrecord\","
                              + "\"fields\":"
                              + "[{\"type\":\"string\",\"name\":"
                              + "\"f" + num + "\"}]}";
        return AvroUtils.parseSchema(schemaString).canonicalString;
    }

    @Override
    protected void doIteration(PerformanceStats.Callback cb) {
        RegisterSchemaRequest registerSchemaRequest = new RegisterSchemaRequest();
        String schema = makeSchema(this.registeredSchemas);
        registerSchemaRequest.setSchema(schema);

        try {
            RestUtils.registerSchema(this.baseUrl, RestUtils.DEFAULT_REQUEST_PROPERTIES,
                                     registerSchemaRequest, this.subject);
            registeredSchemas++;
        } catch(IOException e) {
            System.out.println("Problem registering schema: " + e.getMessage());
        }

        cb.onCompletion(1, 0);
    }

    protected void close() {

    }

    @Override
    protected boolean finished(int iteration) {
        return this.targetRegisteredSchemas == this.registeredSchemas;
    }

    @Override
    protected boolean runningFast(int iteration, float elapsed) {
        return iteration / elapsed > targetSchemasPerSec;
    }
}

