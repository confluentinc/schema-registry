/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.utils;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import kafka.cluster.Broker;
import kafka.zk.BrokerIdZNode;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.kafka.common.config.ConfigException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import io.confluent.common.utils.zookeeper.ZkData;

/**
 * This is a temporary replacement for kafka.utils.ZkUtils and will be removed in CP 6.0
 * when ZooKeeper references are removed from Schema Registry.
 */
public class ZkUtils implements AutoCloseable {

  public static final String BROKERS_PATH = "/brokers";
  public static final String BROKER_IDS_PATH = BROKERS_PATH + "/ids";

  private final ZkClient zkClient;
  private final List<ACL> acls;
  private volatile boolean isNamespacePresent;

  public ZkUtils(String zkConnect, int sessionTimeoutMs, int connectionTimeout, boolean isSecure) {
    ZkConnection zkConnection = new ZkConnection(zkConnect, sessionTimeoutMs);
    this.zkClient = new ZkClient(zkConnection, connectionTimeout, new ZkStringSerializer());
    if (isSecure) {
      acls = new ArrayList<>();
      acls.addAll(ZooDefs.Ids.CREATOR_ALL_ACL);
      acls.addAll(ZooDefs.Ids.READ_ACL_UNSAFE);
    } else {
      acls = Collections.unmodifiableList(ZooDefs.Ids.OPEN_ACL_UNSAFE);
    }
  }

  public ZkClient zkClient() {
    return zkClient;
  }

  public List<Broker> getAllBrokersInCluster() {
    List<String> brokerIds = getChildrenParentMayNotExist(BROKER_IDS_PATH);
    brokerIds.sort(Comparator.naturalOrder());
    return brokerIds.stream()
        .map(Integer::parseInt)
        .map(i -> getBrokerInfo(i))
        .filter(b -> !Objects.isNull(b))
        .collect(Collectors.toList());
  }

  private List<String> getChildrenParentMayNotExist(String path) {
    try {
      return zkClient.getChildren(path);
    } catch (ZkNoNodeException e) {
      return null;
    }
  }

  private Broker getBrokerInfo(int brokerId) {
    String brokerInfo = readDataMaybeNull(BROKER_IDS_PATH + "/" + brokerId).getData();
    return brokerInfo != null ? parseBrokerJson(brokerId, brokerInfo) : null;
  }

  private Broker parseBrokerJson(int id, String jsonString) {
    return BrokerIdZNode.decode(id, jsonString.getBytes(StandardCharsets.UTF_8)).broker();
  }

  public void makeSurePersistentPathExists(String path) {
    if (!zkClient.exists(path)) {
      createPersistent(path, true);
    }
  }

  public void createEphemeralPathExpectConflict(String path, String data) {
    try {
      createEphemeralPath(path, data);
    } catch (ZkNodeExistsException e) {
      // this can happen if there is connection loss; make sure the data is what we intend to write
      String storedData = null;
      try {
        storedData = readData(path).getData();
      } catch (ZkNoNodeException z) {
        // the node disappeared; treat as if node existed and let caller handles this
      }
      if (storedData == null || storedData != data) {
        throw e;
      } else {
        // otherwise, the creation succeeded, return normally
      }
    }
  }

  private void createEphemeralPath(String path, String data) {
    try {
      createEphemeral(path, data);
    } catch (ZkNoNodeException e) {
      createParentPath(path);
      createEphemeral(path, data);
    }
  }

  public void createPersistentPath(String path, String data) {
    try {
      createPersistent(path, data);
    } catch (ZkNoNodeException e) {
      createParentPath(path);
      createPersistent(path, data);
    }
  }

  private void createParentPath(String path) {
    String parentDir = path.substring(0, path.lastIndexOf('/'));
    if (!parentDir.isEmpty()) {
      checkNamespace();
      zkClient.createPersistent(path, true, acls);
    }
  }

  private void checkNamespace() {
    if (!isNamespacePresent && !zkClient.exists("/")) {
      throw new ConfigException("Zookeeper namespace does not exist");
    }
    isNamespacePresent = true;
  }

  public int conditionalUpdatePersistentPath(String path, String data, int expectVersion) {
    try {
      Stat stat = zkClient.writeDataReturnStat(path, data, expectVersion);
      return stat.getVersion();
    } catch (Exception e) {
      return -1;
    }
  }

  public ZkData readData(String path) {
    Stat stat = new Stat();
    String data = zkClient.readData(path, stat);
    return new ZkData(data, stat);
  }

  public ZkData readDataMaybeNull(String path) {
    Stat stat = new Stat();
    String data;
    try {
      data = zkClient.readData(path, stat);
    } catch (ZkNoNodeException e) {
      data = null;
    }
    return new ZkData(data, stat);
  }

  private void createPersistent(String path, Object data) {
    checkNamespace();
    zkClient.createPersistent(path, data, acls);
  }

  private void createPersistent(String path, boolean createParents) {
    checkNamespace();
    zkClient.createPersistent(path, createParents, acls);
  }

  private void createEphemeral(String path, Object data) {
    checkNamespace();
    zkClient.createEphemeral(path, data, acls);
  }

  @Override
  public void close() {
    zkClient.close();
  }

  private static class ZkStringSerializer implements ZkSerializer {

    @Override
    public byte[] serialize(Object data) throws ZkMarshallingError {
      try {
        return ((String) data).getBytes("UTF-8");
      } catch (Exception e) {
        throw new ZkMarshallingError(e);
      }
    }

    @Override
    public Object deserialize(byte[] bytes) throws ZkMarshallingError {
      try {
        return bytes == null ? null : new String(bytes, "UTF-8");
      } catch (Exception e) {
        throw new ZkMarshallingError(e);
      }
    }
  }
}
