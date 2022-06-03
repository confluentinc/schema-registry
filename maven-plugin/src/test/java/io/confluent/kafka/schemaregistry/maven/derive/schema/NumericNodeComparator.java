package io.confluent.kafka.schemaregistry.maven.derive.schema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NumericNode;

import java.util.Comparator;

public class NumericNodeComparator implements Comparator<JsonNode> {
  @Override
  public int compare(JsonNode o1, JsonNode o2) {
    if (o1.equals(o2)) {
      return 0;
    }
    if ((o1 instanceof NumericNode) && (o2 instanceof NumericNode)) {
      Double d1 = ((NumericNode) o1).asDouble();
      Double d2 = ((NumericNode) o2).asDouble();
      if (d1.compareTo(d2) == 0) {
        return 0;
      }
    }
    return 1;
  }
}

