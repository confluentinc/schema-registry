package io.confluent.kafka.schemaregistry.rest.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.Min;

public class Schema {
    @NotEmpty
    private String name;
    @Min(1)
    private Integer version;
    @NotEmpty
    private String schema;
    private boolean compatible;
    private boolean deprecated = false;
    private boolean latest = true;

    public Schema(@JsonProperty("name") String name,
        @JsonProperty("version") Integer version,
        @JsonProperty("schema") String schema,
        @JsonProperty("compatible") boolean compatible,
        @JsonProperty("deprecated") boolean deprecated,
        @JsonProperty("latest") boolean latest) {
        this.name = name;
        this.version = version;
        this.schema = schema;
        this.deprecated = deprecated;
        this.latest = latest;
    }

    @JsonProperty("name")
    public String getName() {
        return name;
    }

    @JsonProperty("name")
    public void setName(String name) {
        this.name = name;
    }

    @JsonProperty("version")
    public Integer getVersion() {
        return this.version;
    }

    @JsonProperty("version")
    public void setVersion(Integer version) {
        this.version = version;
    }

    @JsonProperty("schema")
    public String getSchema() {
        return this.schema;
    }

    @JsonProperty("schema")
    public void setSchema(String schema) {
        this.schema = schema;
    }

    @JsonProperty("compatible")
    public boolean getCompatible() {
        return this.compatible;
    }

    @JsonProperty("compatible")
    public void setCompatible(boolean compatible) {
        this.compatible = compatible;
    }

    @JsonProperty("deprecated")
    public boolean getDeprecated() {
        return this.deprecated;
    }

    @JsonProperty("deprecated")
    public void setDeprecated(boolean deprecated) {
        this.deprecated = deprecated;
    }

    @JsonProperty("latest")
    public boolean getLatest() {
        return this.latest;
    }

    @JsonProperty("latest")
    public void setLatest(boolean latest) {
        this.latest = latest;
    }

}
