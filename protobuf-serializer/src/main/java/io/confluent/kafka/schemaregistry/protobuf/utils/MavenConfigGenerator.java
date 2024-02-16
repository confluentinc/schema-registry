/*
 * Copyright 2023 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.kafka.schemaregistry.protobuf.utils;

import com.squareup.wire.schema.internal.parser.ProtoFileElement;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class MavenConfigGenerator {

  public static void main(String[] args) throws IOException {
    String inputDir = args.length > 0 ? args[0] : ".";
    String outputFile = args.length > 1 ? args[1] : null;
    String replacementChar = args.length > 2 ? args[2] : null;

    Map<Path, List<String>> schemas = new TreeMap<>();
    Path dir = Paths.get(inputDir);
    Files.walk(dir).forEach(path -> {
      processFile(schemas, dir, path);
    });

    PrintWriter printWriter = outputFile != null
        ? new PrintWriter(
            new OutputStreamWriter(new FileOutputStream(outputFile), StandardCharsets.UTF_8))
        : new PrintWriter(
            new OutputStreamWriter(System.out, StandardCharsets.UTF_8));
    printSubjects(printWriter, schemas, replacementChar);
    printSchemaTypes(printWriter, schemas, replacementChar);
    printReferences(printWriter, schemas, replacementChar);
    printWriter.close();
  }

  private static void processFile(Map<Path, List<String>> schemas, Path dir, Path path) {
    try {
      Path relative = dir.relativize(path);
      File file = path.toFile();
      String name = relative.toString();
      if (file.isFile() && name.endsWith(".proto")) {
        ProtoFileElement proto = getSchema(path).rawSchema();
        List<String> imports = proto.getImports();
        schemas.put(relative, imports);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static String toSubject(String path, String replacementChar, boolean inTag) {
    String subject;
    if (path.endsWith(".proto")) {
      subject = path.substring(0, path.length() - ".proto".length());
    } else {
      subject = path;
    }
    if (replacementChar != null) {
      subject = subject.replace(String.valueOf(File.separatorChar), replacementChar);
    } else if (inTag) {
      subject = subject.replace(String.valueOf(File.separatorChar), "_x2F"); // escaped %2F
    }
    return subject;
  }

  private static ProtobufSchema getSchema(Path path) throws IOException {
    String schema = new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
    return new ProtobufSchema(schema);
  }

  private static void printSchemaTypes(
      PrintWriter printWriter, Map<Path, List<String>> schemas, String replacementChar) {
    printWriter.println("<schemaTypes>");
    for (Map.Entry<Path, List<String>> entry : schemas.entrySet()) {
      Path path = entry.getKey();
      String subject = toSubject(path.toString(), replacementChar, true);
      printWriter.print("  <");
      printWriter.print(subject);
      printWriter.print(">PROTOBUF</");
      printWriter.print(subject);
      printWriter.println(">");
    }
    printWriter.println("</schemaTypes>");
  }

  private static void printSubjects(
      PrintWriter printWriter, Map<Path, List<String>> schemas, String replacementChar) {
    printWriter.println("<subjects>");
    for (Map.Entry<Path, List<String>> entry : schemas.entrySet()) {
      Path path = entry.getKey();
      String subject = toSubject(path.toString(), replacementChar, true);
      printWriter.print("  <");
      printWriter.print(subject);
      printWriter.print(">");
      printWriter.print(path);
      printWriter.print("</");
      printWriter.print(subject);
      printWriter.println(">");
    }
    printWriter.println("</subjects>");
  }

  private static void printReferences(
      PrintWriter printWriter, Map<Path, List<String>> schemas, String replacementChar) {
    printWriter.println("<references>");
    for (Map.Entry<Path, List<String>> entry : schemas.entrySet()) {
      Path path = entry.getKey();
      List<String> imports = entry.getValue();
      String subject = toSubject(path.toString(), replacementChar, true);
      if (imports.isEmpty()) {
        printWriter.print("  <");
        printWriter.print(subject);
        printWriter.println("/>");
      } else {
        printWriter.print("  <");
        printWriter.print(subject);
        printWriter.println(">");
        for (String dep : imports) {
          printWriter.println("    <reference>");
          printWriter.print("      <name>");
          printWriter.print(dep);
          printWriter.println("</name>");
          printWriter.print("      <subject>");
          printWriter.print(toSubject(dep, replacementChar, false));
          printWriter.println("</subject>");
          printWriter.println("    </reference>");

        }
        printWriter.print("  </");
        printWriter.print(subject);
        printWriter.println(">");
      }
    }
    printWriter.println("</references>");
  }
}
