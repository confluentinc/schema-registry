package io.confluent.connect.avro;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

import io.test.avro.core.AvroMessage;
import io.test.avro.doc.DocTestRecord;
import io.test.avro.union.FirstOption;
import io.test.avro.union.MultiTypeUnionMessage;
import io.test.avro.union.SecondOption;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.Union;
import org.apache.avro.specific.SpecificData;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import uk.co.jemos.podam.api.PodamFactory;
import uk.co.jemos.podam.api.PodamFactoryImpl;

public class AdditionalAvroDataTest
{
    private AvroData avroData;
    @Before
    public void before(){
        AvroDataConfig avroDataConfig = new AvroDataConfig.Builder()
                                                   .with(AvroDataConfig.SCHEMAS_CACHE_SIZE_CONFIG, 1)
                                                   .with(AvroDataConfig.CONNECT_META_DATA_CONFIG, false)
                                                   .with(AvroDataConfig.ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG, true)
                                                   .build();

        avroData = new AvroData(avroDataConfig);
    }


    @Test
    public void testDocumentationPreservedSchema() throws IOException
    {
        Schema avroSchema = new Parser().parse(new File("src/test/avro/DocTestRecord.avsc"));

        org.apache.kafka.connect.data.Schema connectSchema = avroData.toConnectSchema(avroSchema,
            new HashMap<>(), new HashSet<>());

        Schema outputAvroSchema = avroData.fromConnectSchema(connectSchema);

        Assert.assertEquals(avroSchema, outputAvroSchema);
    }

    @Test
    public void testDocumentationPreservedData() throws IOException
    {
        PodamFactory factory = new PodamFactoryImpl();

        DocTestRecord testRecord = factory.manufacturePojo(DocTestRecord.class);
        org.apache.kafka.connect.data.SchemaAndValue connectSchemaAndValue = avroData.toConnectData(testRecord.getSchema(), testRecord);

        Object output = avroData.fromConnectData(connectSchemaAndValue.schema(), connectSchemaAndValue.value());

        Assert.assertEquals(SpecificData.get().toString(testRecord), SpecificData.get().toString(output));
    }
    
    @Test
    public void testComplexUnionSchema() throws IOException
    {
        // Here is a schema complex union schema
        Schema avroSchema = new Parser().parse(new File("src/test/avro/AvroMessage.avsc"));

        org.apache.kafka.connect.data.Schema connectSchema = avroData.toConnectSchema(avroSchema, new HashMap<>(), new HashSet<>());

        Schema outputAvroSchema = avroData.fromConnectSchema(connectSchema);

        Assert.assertEquals(avroSchema, outputAvroSchema);
    }

    @Test
    public void testComplexUnionData() throws IOException
    {
        PodamFactory factory = new PodamFactoryImpl();

        AvroMessage avroMessage = factory.manufacturePojo(AvroMessage.class);
        org.apache.kafka.connect.data.SchemaAndValue connectSchemaAndValue = avroData.toConnectData(avroMessage.getSchema(), avroMessage);

        Object output = avroData.fromConnectData(connectSchemaAndValue.schema(), connectSchemaAndValue.value());

        Assert.assertEquals(SpecificData.get().toString(avroMessage), SpecificData.get().toString(output));
    }


    @Test
    public void testComplexMultiUnionData() throws IOException
    {
        PodamFactory factory = new PodamFactoryImpl();

        MultiTypeUnionMessage avroMessage = factory.manufacturePojo(MultiTypeUnionMessage.class);

        org.apache.kafka.connect.data.SchemaAndValue connectSchemaAndValue = avroData.toConnectData(avroMessage.getSchema(), avroMessage);
        Object output = avroData.fromConnectData(connectSchemaAndValue.schema(), connectSchemaAndValue.value());
        Assert.assertEquals(SpecificData.get().toString(avroMessage), SpecificData.get().toString(output));

        avroMessage.setCompositeRecord(new FirstOption("x",2l));
        connectSchemaAndValue = avroData.toConnectData(avroMessage.getSchema(), avroMessage);
        output = avroData.fromConnectData(connectSchemaAndValue.schema(), connectSchemaAndValue.value());
        Assert.assertEquals(SpecificData.get().toString(avroMessage), SpecificData.get().toString(output));

        avroMessage.setCompositeRecord(new SecondOption("y",3l));
        connectSchemaAndValue = avroData.toConnectData(avroMessage.getSchema(), avroMessage);
        output = avroData.fromConnectData(connectSchemaAndValue.schema(), connectSchemaAndValue.value());
        Assert.assertEquals(SpecificData.get().toString(avroMessage), SpecificData.get().toString(output));

        avroMessage.setCompositeRecord(Arrays.asList(new String[]{"1","2"}));
        connectSchemaAndValue = avroData.toConnectData(avroMessage.getSchema(), avroMessage);
        output = avroData.fromConnectData(connectSchemaAndValue.schema(), connectSchemaAndValue.value());
        Assert.assertEquals(SpecificData.get().toString(avroMessage), SpecificData.get().toString(output));

    }
    @Test
    /**
     * @see https://github.com/confluentinc/schema-registry/issues/405
     */
    public void testNestedUnion() {
        // Cannot use AllowNull to generate schema
        // because Avro 1.7.7 will throw org.apache.avro.AvroRuntimeException: Nested union
        // Schema myAvroObjectSchema = AllowNull.get().getSchema(MyObjectToPersist.class);

        // Here is a schema generated by Avro 1.8.1
        Schema myAvroObjectSchema = new Parser().parse(
           "{"
           + "  \"type\" : \"record\","
           + "  \"name\" : \"MyObjectToPersist\","
           + "  \"namespace\" : \"io.confluent.connect.avro.AdditionalAvroDataTest$\","
           + "  \"fields\" : [ {"
           + "    \"name\" : \"obj\","
           + "    \"type\" : [ \"null\", {"
           + "      \"type\" : \"record\","
           + "      \"name\" : \"MyImpl1\","
           + "      \"fields\" : [ {"
           + "        \"name\" : \"data\","
           + "        \"type\" : [ \"null\", \"string\" ],"
           + "        \"default\" : null"
           + "      } ]"
           + "    }, {"
           + "      \"type\" : \"record\","
           + "      \"name\" : \"MyImpl2\","
           + "      \"fields\" : [ {"
           + "        \"name\" : \"data\","
           + "        \"type\" : [ \"null\", \"string\" ],"
           + "        \"default\" : null"
           + "      } ]"
           + "    } ],"
           + "    \"default\" : null"
           + "  } ]"
           + "}");
        Schema myImpl1Schema = ReflectData.AllowNull.get().getSchema(MyImpl1.class);
        GenericData.Record nestedRecord = new GenericRecordBuilder(myImpl1Schema).set("data", "mydata").build();
        GenericData.Record obj = new GenericRecordBuilder(myAvroObjectSchema).set("obj", nestedRecord).build();

        org.apache.kafka.connect.data.Schema connectSchema = avroData.toConnectSchema(myAvroObjectSchema, new HashMap<>(), new HashSet<>());
        SchemaAndValue schemaAndValue = avroData.toConnectData(myAvroObjectSchema, obj);
        Object o = avroData.fromConnectData(schemaAndValue.schema(), schemaAndValue.value());
        Assert.assertEquals(obj ,o);
        avroData.fromConnectSchema(connectSchema);
    }

    @Union({MyImpl1.class, MyImpl2.class})
    interface MyInterface {}
    static class MyImpl1 implements MyInterface
    { private String data; }
    static class MyImpl2 implements MyInterface
    { private String data; }
    static class MyObjectToPersist { private MyInterface obj; }
}