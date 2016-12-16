package io.confluent.connect.avro;

import java.util.HashMap;
import java.util.Map;

import io.confluent.common.config.AbstractConfig;
import io.confluent.common.config.ConfigDef;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;

public class AvroConverterConfig extends AbstractKafkaAvroSerDeConfig
{
   public AvroConverterConfig(Map<?, ?> props)
   {
      super(baseConfigDef(), props);
   }

   public static class Builder {

      private Map<String, Object> props = new HashMap<>();

      public Builder with(String key, Object value){
         props.put(key, value);
         return this;
      }

      public AvroConverterConfig build(){
         return new AvroConverterConfig(props);
      }
   }
}
