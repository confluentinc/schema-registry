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

/**
 * Supplies the {@link SSLSocketFactory} used by the follower-&gt;leader forwarding client and owns
 * the lifecycle of the underlying credential source.
 *
 * <p>This is the seam through which commercial builds plug in an alternative source of client TLS
 * credentials (for example SPIRE/SPIFFE mTLS, where the X.509 SVID is obtained from the Workload
 * API and rotated automatically in memory) without the open-source core depending on those
 * implementations. See {@link KafkaSchemaRegistry#createLeaderForwardingClient}.
 */
public interface LeaderForwardingClient extends Closeable {

  /**
   * Returns the socket factory to use for the forwarding connection, or {@code null} to fall back
   * to the standard keystore-based TLS.
   */
  SSLSocketFactory sslSocketFactory();
}
