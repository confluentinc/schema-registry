package io.confluent.kafka.schemaregistry.zookeeper;

import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import io.confluent.kafka.schemaregistry.storage.KafkaSchemaRegistry;
import io.confluent.kafka.schemaregistry.storage.exceptions.SchemaRegistryException;
import kafka.utils.ZkUtils;

public class ZookeeperMasterElector {
  private static final Logger log = LoggerFactory.getLogger(ZookeeperMasterElector.class);
  private static final String MASTER_PATH = "/schema-registry-master";

  private final ZkClient zkClient;
  private final SchemaRegistryIdentity myIdentity;
  private final String myIdentityString;
  private final KafkaSchemaRegistry schemaRegistry;


  public ZookeeperMasterElector(ZkClient zkClient, SchemaRegistryIdentity myIdentity, KafkaSchemaRegistry schemaRegistry)
    throws SchemaRegistryException {
    this.zkClient = zkClient;
    this.myIdentity = myIdentity;
    try {
      this.myIdentityString = myIdentity.toJson();
    } catch (IOException e) {
      throw new SchemaRegistryException("Error while serializing " + myIdentity.toString(),
                                        e);
    }
    this.schemaRegistry = schemaRegistry;
  }

  public void init() {
    zkClient.subscribeStateChanges(new SessionExpirationListener());
    zkClient.subscribeDataChanges(MASTER_PATH, new MasterChangeListener());
    electMaster();
  }

  public void close() {
    zkClient.unsubscribeAll();
  }

  public void electMaster() {
    SchemaRegistryIdentity masterIdentity = null;
    try {
      ZkUtils.createEphemeralPathExpectConflict(zkClient, MASTER_PATH, myIdentityString);
      log.info("Successfully elected the new master: " + myIdentityString);
      masterIdentity = myIdentity;
    } catch (ZkNodeExistsException znee) {
      // If someone else has written the path, read the new master back
      try {
        String masterIdentityString = ZkUtils.readData(zkClient, MASTER_PATH)._1();
        try {
          masterIdentity = SchemaRegistryIdentity.fromJson(masterIdentityString);
        } catch (IOException ioe) {
          log.error("Can't parse schema registry identity string " + masterIdentityString);
        }
      } catch (ZkNoNodeException znne) {
        // just let it go; we will get another handleDataDeleted event
      }
    }
    schemaRegistry.setMaster(masterIdentity);
  }

  private class MasterChangeListener implements IZkDataListener {
    /**
     * Called when the leader information stored in zookeeper has changed. Record the new leader in memory
     * @throws Exception On any error.
     */
    @Override
    public void handleDataChange(String dataPath, Object data) {
      // no need to handle
    }

    /**
     * Called when the leader information stored in zookeeper has been delete. Try to elect as the leader
     * @throws Exception On any error.
     */
    @Override
    public void handleDataDeleted(String dataPath) {
      electMaster();
    }
  }

  private class SessionExpirationListener implements IZkStateListener {

    @Override
    public void handleStateChanged(Watcher.Event.KeeperState state) {
      // do nothing, since zkclient will do reconnect for us.
    }

    /**
     * Called after the zookeeper session has expired and a new session has been created. You would have to re-create
     * any ephemeral nodes here.
     *
     * @throws Exception On any error.
     */
    @Override
    public void handleNewSession() {
      electMaster();
    }
  }
}
