/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.kafka.schemaregistry.maven;

import io.confluent.kafka.schemaregistry.maven.derive.schema.DeriveSchemaMain;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.json.JSONObject;

import java.io.FileOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static io.confluent.kafka.schemaregistry.maven.derive.schema.utils.ReadFileUtils.readMessagesToString;

@Mojo(name = "derive-schema", configurator = "custom-basic")
public class DeriveSchemaMojo extends AbstractMojo {

  @Parameter(required = true)
  File messagePath;

  @Parameter(defaultValue = "null")
  File outputPath;

  @Parameter(defaultValue = "Avro")
  String schemaType;

  @Parameter(defaultValue = "true")
  boolean strictCheck;

  @Override
  public void execute() throws MojoExecutionException, MojoFailureException {

    ArrayList<String> listOfMessages;
    try {
      listOfMessages = new ArrayList<>(readMessagesToString(messagePath));
    } catch (IOException e) {
      throw new MojoExecutionException(e.getMessage());
    }

    try {
      List<JSONObject> ans = DeriveSchemaMain.caseWiseOutput(schemaType, strictCheck,
          listOfMessages);

      if (outputPath == null) {
        for (JSONObject schema : ans) {
          System.out.println(schema.toString(2));
        }
      } else {

        try {
          FileOutputStream fileStream = new FileOutputStream(outputPath.getPath());
          OutputStreamWriter writer = new OutputStreamWriter(fileStream,
              StandardCharsets.UTF_8);
          for (JSONObject schema : ans) {
            writer.write(schema.toString(2));
          }
          writer.close();
        } catch (IOException e) {
          getLog().error(e.getMessage());
          throw new MojoExecutionException(e.getMessage());
        }

      }
    } catch (IOException e) {
      getLog().error(e.getMessage());
      throw new MojoExecutionException(e.getMessage());
    }

  }

}
