package io.confluent.kafka.schemaregistry.utils;

public class Pair<K,V> {

  private K item1;
  private V item2;

  public Pair(K item1, V item2) {
    this.item1 = item1;
    this.item2 = item2;
  }

  public K getFirst() {
    return this.item1;
  }

  public V getLast() {
    return this.item2;
  }
}
