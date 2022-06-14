/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.maven.derive.schema.utils;

import static java.lang.Math.min;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.confluent.kafka.schemaregistry.maven.derive.schema.DeriveSchema.mapper;

/**
 * Utility class for reading files and strings according to input format
 */
public class ReadFileUtils {

  private static final Logger logger = LoggerFactory.getLogger(ReadFileUtils.class);

  public static String readFile(String content) throws IOException {
    byte[] encoded = Files.readAllBytes(Paths.get(content));
    return new String(encoded, StandardCharsets.UTF_8);
  }

  /**
   * Given a file with array of ObjectNodes, reads it and generates List of ObjectNodes.
   * <p>
   * Eg, Expected File contents are :
   * [{"name":"J"}, {"name":"K"}, {"name":"L"}]
   * </p>
   *
   * @param content Content or name of the file to read
   * @return List of ObjectNodes
   */
  public static List<ObjectNode> readArrayOfMessages(String content)
      throws JsonProcessingException {

    ArrayNode arrayNode = mapper.readValue(content, ArrayNode.class);
    List<ObjectNode> listOfMessages = new ArrayList<>();
    for (int i = 0; i < arrayNode.size(); i++) {
      listOfMessages.add((ObjectNode) arrayNode.get(i));
    }
    return listOfMessages;
  }

  /**
   * Given a file with ObjectNodes which are line separated,
   * list of ObjectNodes are generated and returned.
   * Eg, Expected File contents are :
   * <p>
   * {"name":"J"}
   * </p>
   * <p>
   * {"name":"K"}
   * </p>
   * <p>
   * {"name":"L"}
   * </p>
   * </p>
   *
   * @param content Name of the file to read
   * @return List of ObjectNodes
   */
  public static List<Object> readLinesOfMessages(String content) throws JsonProcessingException {

    List<Object> listOfMessages = new ArrayList<>();
    String[] lineSeparatedMessages = content.split("\n");
    for (String line : lineSeparatedMessages) {
      listOfMessages.add(mapper.readValue(line, ObjectNode.class));
    }

    return listOfMessages;
  }

  private static void checkEmpty(String content) {
    if (content == null || content.length() == 0) {
      throw new IllegalArgumentException("Input file is empty.");
    }
  }

  private static List<Object> readCustom(String content) {

    checkEmpty(content);
    List<Object> messageList = new ArrayList<>();

    try {
      messageList.addAll(readArrayOfMessages(content));
      logger.info("Read input as array of Messages.");
      return messageList;
    } catch (Exception ignored) {
      logger.warn("Cannot be read input as array of Messages.");
    }

    try {
      messageList.addAll(readLinesOfMessages(content));
      logger.info("Read input as lines of messages.");
      return messageList;
    } catch (Exception ignored) {
      logger.warn("Cannot be read input as lines of messages.");
    }

    try {
      messageList.add(mapper.readValue(content, ObjectNode.class));
      logger.info("Read input as ObjectNode.");
      return messageList;
    } catch (Exception ignored) {
      logger.warn("Cannot be read input as ObjectNode.");
    }

    if (messageList.isEmpty()) {
      logger.error("Unable to read messages.");
      throw new IllegalArgumentException("Input file format not understood.");
    }

    return messageList;
  }

  public static List<String> readMessagesToString(String content) {

    logger.info(String.format("Reading input, Substring 0-20 is :%n  %s",
        content.substring(0, min(20, content.length()))));

    List<Object> listOfMessages = readCustom(content);
    List<String> listOfStrings = new ArrayList<>();
    for (Object message : listOfMessages) {
      listOfStrings.add(message.toString());
    }
    return listOfStrings;
  }

  public static List<String> readMessagesToString(File file) throws IOException {

    if (file == null) {
      throw new NullPointerException("Input file not set.");
    }
    logger.info(String.format("Reading input file %s", file.getName()));
    String fileContent = readFile(file.getAbsolutePath());
    return readMessagesToString(fileContent);
  }

}