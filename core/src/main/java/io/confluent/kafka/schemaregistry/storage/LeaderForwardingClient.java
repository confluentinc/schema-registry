/*
 * Copyright 2026 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.storage;

import java.io.Closeable;
import javax.net.ssl.SSLSocketFactory;

public interface LeaderForwardingClient extends Closeable {

  /**
   * Returns the socket factory to use for the forwarding connection, or {@code null} to fall back
   * to the standard keystore-based TLS.
   */
  SSLSocketFactory sslSocketFactory();
}
