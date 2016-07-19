package io.confluent.kafka.schemaregistry.maven;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

@Mojo(name = "register")
public class RegisterSchemaRegistryMojo extends SchemaRegistryMojo {


  @Parameter(required = true)
  Map<String, File> subjects = new HashMap<>();


  Map<String, Integer> schemaVersions;

  @Override
  public void execute() throws MojoExecutionException, MojoFailureException {
    Map<String, Schema> subjectToSchemaLookup = loadSchemas(this.subjects);
    this.schemaVersions = new LinkedHashMap<>();

    for (Map.Entry<String, Schema> kvp : subjectToSchemaLookup.entrySet()) {
      try {
        if (getLog().isDebugEnabled()) {
          getLog().debug(
              String.format("Calling register('%s', '%s')", kvp.getKey(), kvp.getValue().toString(true))
          );
        }

        Integer version = this.client().register(kvp.getKey(), kvp.getValue());
        getLog().info(
            String.format(
                "Registered subject(%s) with version %s",
                kvp.getKey(),
                version
            ));
        this.schemaVersions.put(kvp.getKey(), version);
      } catch (IOException | RestClientException e) {
        getLog().error(
            String.format("Exception thrown while registering subject(%s)", kvp.getKey()),
            e
        );
      }
    }
  }
}
