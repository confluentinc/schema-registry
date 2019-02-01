package io.confluent.connect.avro;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.SchemaBuilder;
import org.junit.Test;

import javax.xml.bind.DatatypeConverter;
import java.math.BigDecimal;

import static org.junit.Assert.assertEquals;

public class DecimalTest {
    private AvroData avroData = new AvroData(2);

    @Test
    public void testDecimalDefaultValue() {
        Schema decimalSchema = LogicalTypes.decimal(12, 5)
                .addToSchema(Schema.create(Type.BYTES));

        BigDecimal bigDecimal = new BigDecimal("314.12345");
        byte[] defaultBigIntegerByteArray = bigDecimal.unscaledValue().toByteArray();
        String b64String = DatatypeConverter.printBase64Binary(defaultBigIntegerByteArray);

        Schema avroSchema = SchemaBuilder
                .record("testRecord")
                .namespace("org.example")
                .fields()
                .name("number")
                .type(decimalSchema)
                .withDefault(b64String)
                .endRecord();

        org.apache.kafka.connect.data.Schema kafkaSchema = avroData.toConnectSchema(avroSchema);

        BigDecimal actualBigDecimal = (BigDecimal) kafkaSchema.field("number").schema().defaultValue();
        assertEquals(bigDecimal, actualBigDecimal);
    }
}
