/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.confluent.kafka.schemaregistry.avro;

import org.apache.avro.Schema;


import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Collection of static methods for generating the canonical form of schemas.
 *
 * Code lifted from {@link org.apache.avro.SchemaNormalization}.
 */
public class SchemaNormalizationWithDefault {

    private SchemaNormalizationWithDefault() {}

    /** Returns "Parsing Canonical Form" of a schema as defined by Avro
     * spec, with the addition of 'default' field attributes. */
    public static String toCanonicalForm(Schema s) {
        try {
            Map<String,String> env = new HashMap<String,String>();
            return build(env, s, new StringBuilder()).toString();
        } catch (IOException e) {
            // Shouldn't happen, b/c StringBuilder can't throw IOException
            throw new RuntimeException(e);
        }
    }

    private static Appendable build(Map<String,String> env, Schema s,
                                    Appendable o) throws IOException {
        boolean firstTime = true;
        Schema.Type st = s.getType();
        switch (st) {
            default: // boolean, bytes, double, float, int, long, null, string
                if (s.getLogicalType()!=null)
                {
                    o.append("{\"type\":\"").append(st.getName()).append("\"");
                    o.append(",\"logicalType\":")
                            .append('"')
                            .append(s.getLogicalType().getName())
                            .append('"');
                    return o.append("}");
                }
                else if (s.getObjectProp("connect.name")!= null){
                    switch (s.getObjectProp("connect.name").toString()) {
                        default:
                            return o.append('"').append(st.getName()).append('"');
                        case "io.debezium.time.Date":
                            o.append("{\"type\":\"").append(st.getName()).append("\"");
                            o.append(",\"logicalType\":")
                                    .append('"')
                                    .append("date")
                                    .append('"');
                            return o.append("}");
                        case "io.debezium.time.Timestamp":
                            o.append("{\"type\":\"").append(st.getName()).append("\"");
                            o.append(",\"logicalType\":")
                                    .append('"')
                                    .append("timestamp-millis")
                                    .append('"');
                            return o.append("}");
                        case "io.debezium.time.MicroTime":
                            o.append("{\"type\":\"").append(st.getName()).append("\"");
                            o.append(",\"logicalType\":")
                                    .append('"')
                                    .append("time-micros")
                                    .append('"');
                            return o.append("}");
                        case "io.debezium.time.MicroTimestamp":
                            o.append("{\"type\":\"").append(st.getName()).append("\"");
                            o.append(",\"logicalType\":")
                                    .append('"')
                                    .append("timestamp-micros")
                                    .append('"');
                            return o.append("}");
                        case "io.debezium.time.Time":
                            o.append("{\"type\":\"").append(st.getName()).append("\"");
                            o.append(",\"logicalType\":")
                                    .append('"')
                                    .append("time-millis")
                                    .append('"');
                            return o.append("}");
                    }
                }
                else {
                    return o.append('"').append(st.getName()).append('"');
                }
            case UNION:
                o.append('[');
                for (Schema b: s.getTypes()) {
                    if (! firstTime) o.append(','); else firstTime = false;
                    build(env, b, o);
                }
                return o.append(']');

            case ARRAY:  case MAP:
                o.append("{\"type\":\"").append(st.getName()).append("\"");
                if (st == Schema.Type.ARRAY)
                    build(env, s.getElementType(), o.append(",\"items\":"));
                else build(env, s.getValueType(), o.append(",\"values\":"));
                return o.append("}");

            case ENUM: case FIXED: case RECORD:
                String name = s.getFullName();
                if (env.get(name) != null) return o.append(env.get(name));
                String qname = "\""+name+"\"";
                env.put(name, qname);
                o.append("{\"name\":").append(qname);
                o.append(",\"type\":\"").append(st.getName()).append("\"");
                if (st == Schema.Type.ENUM) {
                    o.append(",\"symbols\":[");
                    for (String enumSymbol: s.getEnumSymbols()) {
                        if (! firstTime) o.append(','); else firstTime = false;
                        o.append('"').append(enumSymbol).append('"');
                    }
                    o.append("]");
                } else if (st == Schema.Type.FIXED) {
                    o.append(",\"size\":").append(Integer.toString(s.getFixedSize()));
                } else { // st == Schema.Type.RECORD
                    o.append(",\"fields\":[");
                    for (Schema.Field f: s.getFields()) {
                        if (! firstTime) o.append(','); else firstTime = false;
                        o.append("{\"name\":\"").append(f.name()).append("\"");
                        build(env, f.schema(), o.append(",\"type\":"));
                        if (f.defaultValue() != null) {
                            o.append(",\"default\":")
                                    .append(f.defaultValue().toString())
                                    .append("");
                        }
                        o.append("}");
                    }
                    o.append("]");
                }
                return o.append("}");
        }
    }

}