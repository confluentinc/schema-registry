package io.confluent.kafka.schemaregistry.maven;

public class SubjectVersion {
    public final String subject;
    public final int version;

    public SubjectVersion(String subject, int version) {
        this.subject = subject;
        this.version = version;
    }
}
