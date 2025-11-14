/*
 * Copyright 2025 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.kafka.schemaregistry;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;

/**
 * Interface defining common operations needed by schema registry integration tests.
 * This allows tests to be written and run against an arbitrary test harness.
 */
public interface SchemaRegistryTestHarness {
  /**
   * Gets the REST application instance for making schema registry API calls.
   * @return RestApp instance
   */
  RestApp getRestApp();
  
  /**
   * Gets the port on which the Schema Registry is listening.
   * @return schema registry port number
   */
  Integer getSchemaRegistryPort();

  /**
   * Chooses an available port for schema registry or other services.
   * @return available port number
   */
  default int choosePort() {
    return choosePorts(1)[0];
  }


  /**
   * Choose a number of random available ports
   */
  static int[] choosePorts(int count) {
    try {
      ServerSocket[] sockets = new ServerSocket[count];
      int[] ports = new int[count];
      for (int i = 0; i < count; i++) {
        sockets[i] = new ServerSocket(0, 0, InetAddress.getByName("0.0.0.0"));
        ports[i] = sockets[i].getLocalPort();
      }
      for (int i = 0; i < count; i++) {
        sockets[i].close();
      }
      return ports;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Gets the schema registry protocol (http or https).
   * @return protocol string
   */
  String getSchemaRegistryProtocol();
}

