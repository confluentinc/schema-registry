package io.confluent.kafka.schemaregistry.storage.encoder;

import io.confluent.resourcemanager.api.client.ClientImpl;
import io.confluent.resourcemanager.api.model.ObjectList;
import io.confluent.resourcemanager.api.model.identifier.ObjectIdentifier;
import io.confluent.resourcemanager.api.model.location.Location;
import io.confluent.resourcemanager.api.model.scope.ScopingAttribute;
import io.confluent.resourcemanager.protobuf.apis.meta.v1.ScopeAttribute;

import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.List;

public class MetadataEncoderRMService {

  private final ClientImpl client;
  private final String orgName;

  public MetadataEncoderRMService(String orgName) {
    this.orgName = orgName;
    this.client = new ClientImpl(
            "http://localhost:8080",  // httpHost
            "localhost:50051"         // grpcHost
    );
  }

  public CompletableFuture<MetadataKeysetWrapper> createKeysetWrapper(MetadataKeysetWrapper keysetWrapper) {
    return client.create(keysetWrapper).execute();
  }

  public CompletableFuture<Optional<MetadataKeysetWrapper>> getKeysetWrapper(String name) {
    ObjectIdentifier<MetadataKeysetWrapper> identifier = new ObjectIdentifier<>(
            Location.Default(),
            Collections.singletonList(ScopeAttribute.newBuilder()
                    .setName(ScopingAttribute.ORG.scopeName())
                    .setValue(orgName)
                    .build()),
            name,
            MetadataKeysetWrapper.class
    );
    return client.get(identifier).execute();
  }

  public CompletableFuture<Optional<MetadataKeysetWrapper>> updateKeysetWrapper(MetadataKeysetWrapper keysetWrapper) {
    return client.update(keysetWrapper).execute();
  }

  public CompletableFuture<Optional<MetadataKeysetWrapper>> deleteKeysetWrapper(String name) {
    ObjectIdentifier<MetadataKeysetWrapper> identifier = new ObjectIdentifier<>(
            Location.Default(),
            Collections.singletonList(ScopeAttribute.newBuilder()
                    .setName(ScopingAttribute.ORG.scopeName())
                    .setValue(orgName)
                    .build()),
            name,
            MetadataKeysetWrapper.class
    );
    return client.delete(identifier).execute();
  }

  public CompletableFuture<ObjectList<MetadataKeysetWrapper>> listKeysetWrappers() {
    return client.list(MetadataKeysetWrapper.class).execute();
  }
}
