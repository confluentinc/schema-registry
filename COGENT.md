# Header-Based Avro Subject Routing for Kafka Connect

## What This Does
Routes Kafka messages to different Avro schema subjects based on message headers.

- **Header:** `cogent_extraction_avro_subject_name` 
- **If header present:** Use header value as schema subject
- **If header missing:** Route to DLQ subject `__cogent_extraction_avro_subject_name_unavailable__`

## Quick Start

### 1. Build
```bash
mvn clean package -pl avro-converter -DskipTests -Dcheckstyle.skip=true -Dspotbugs.skip=true
```

### 2. Test
```bash
mvn test -pl avro-converter -Dtest=HeaderBasedAvroConverterTest#testOriginalCogentScenario -Dcheckstyle.skip=true -Dspotbugs.skip=true
```
✅ **Test passes!** Confirms header-based routing works.

### 3. Deploy
```bash
git add .
git commit -m "Add header-based subject routing for Kafka Connect"
git tag v7.6.0-cogent-2
git push origin cogent-header-deserializer
git push origin v7.6.0-cogent-2
```

## Usage in MSK Connect

**Replace your standard AvroConverter with:**

```properties
value.converter=com.cogent.kafka.connect.HeaderBasedAvroConverter
value.converter.schema.registry.url=https://psrc-0kywq.us-east-2.aws.confluent.cloud
value.converter.schema.registry.basic.auth.credentials.source=USER_INFO
value.converter.schema.registry.basic.auth.user.info=YOUR_CREDENTIALS
value.converter.schemas.enable=true
```

That's it! Messages will now route to schema subjects based on their headers.

---

## Example Scenario

**Topic:** `pacific-m365-user`  
**Header:** `cogent_extraction_avro_subject_name=m365-user-value`  
**Result:** Message uses schema from subject `m365-user-value`

**Topic:** `pacific-m365-user`  
**Header:** *(missing)*  
**Result:** Message routed to DLQ with subject `__cogent_extraction_avro_subject_name_unavailable__`

---

## Files Created
- `avro-converter/src/main/java/com/cogent/kafka/connect/HeaderBasedAvroConverter.java`
- `avro-converter/src/test/java/com/cogent/kafka/connect/HeaderBasedAvroConverterTest.java`
