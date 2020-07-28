package io.confluent.kafka.schemaregistry.tools;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.schemaregistry.storage.*;
import io.confluent.kafka.schemaregistry.storage.exceptions.SerializationException;
import io.confluent.kafka.schemaregistry.storage.serialization.SchemaRegistrySerializer;
import io.confluent.kafka.schemaregistry.storage.serialization.Serializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Properties;
import java.util.Scanner;

public class RestoreFromBackup {
    private static final Logger log = LoggerFactory.getLogger(RestoreFromBackup.class);
    private static final String topic = "_schemas";
    private static final Serializer<SchemaRegistryKey, SchemaRegistryValue> serializer = new SchemaRegistrySerializer();

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println(
                    "Usage: java " + RestoreFromBackup.class.getName() + " backup file"
                            + " bootstrap brokers (space separated list)"
            );
            System.exit(1);
        }
        for (int i = 0; i < args.length; i++) {
            System.out.println(args[i]);
        }

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, args[1]);
        props.put(ProducerConfig.ACKS_CONFIG, "-1");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.ByteArraySerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.ByteArraySerializer.class);
        props.put(ProducerConfig.RETRIES_CONFIG, 0); // Producer should not retry
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "backup-restore-producer");

        KafkaProducer<byte[], byte[]> producer = new KafkaProducer<byte[], byte[]>(props);

        restoreFromFilePath(args[0], producer);
    }
    private static void restoreFromFilePath(String filePath, KafkaProducer<byte[], byte[]> producer) {
        try {
            Scanner scanner = new Scanner(new File(filePath), "UTF-8");
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                String[] tokens = line.split("\t");
                if (tokens.length != 4) {
                    log.error("wrong number of parts for line");
                }
                ObjectMapper obj = new ObjectMapper();
                //TODO: not just hardcode this
                SchemaRegistryKey key = obj.readValue(tokens[0], SchemaKey.class);
                SchemaRegistryValue value = obj.readValue(tokens[1], SchemaValue.class);
                String[] tpTokens = tokens[2].split("-");
                TopicPartition tp = new TopicPartition(tpTokens[0], Integer.parseInt(tpTokens[1]));
                long timestamp = Long.parseLong(tokens[3]);
                ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<byte[], byte[]>(
                        topic,
                        tp.partition(),
                        timestamp,
                        serializer.serializeKey(key),
                        value == null ? null : serializer.serializeValue(value));
                producer.send(producerRecord).get();
            }
            scanner.close();
        } catch (FileNotFoundException e) {
            log.error("file not found");
        } catch (JsonProcessingException e) {
            log.error("json processing exception");
        } catch (SerializationException e) {
            log.error("serialization error");
        } catch (Exception e) {
            log.error("other exception");
        }
    }
}
