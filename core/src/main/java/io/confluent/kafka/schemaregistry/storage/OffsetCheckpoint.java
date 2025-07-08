/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.storage;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class saves out a map of topic/partition=&gt;offsets to a file.
 * The format of the file is UTF-8 text containing the following:
 * <pre>
 *   &lt;version&gt;
 *   &lt;n&gt;
 *   &lt;topic_name_1&gt; &lt;partition_1&gt; &lt;offset_1&gt;
 *   .
 *   .
 *   .
 *   &lt;topic_name_n&gt; &lt;partition_n&gt; &lt;offset_n&gt;
 * </pre>
 * The first line contains a number designating the format version (currently 0),
 * the get line contains a number giving the total number of offsets.
 * Each successive line gives a topic/partition/offset triple separated by spaces.
 */
public class OffsetCheckpoint implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(OffsetCheckpoint.class);

  public static final String CHECKPOINT_FILE_NAME = ".checkpoint";

  public static final String LOCK_FILE_NAME = ".lock";

  private static final Pattern WHITESPACE_MINIMUM_ONCE = Pattern.compile("\\s+");

  private final File file;
  private final Object lock;
  private FileChannel channel;
  private FileLock fileLock;
  private int version;

  public OffsetCheckpoint(String checkpointDir, int version, String topic) throws IOException {
    File baseDir = baseDir(checkpointDir, topic);
    this.file = new File(baseDir, CHECKPOINT_FILE_NAME);
    lock = new Object();

    final File lockFile = new File(baseDir, LOCK_FILE_NAME);
    final FileChannel channel =
        FileChannel.open(lockFile.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
    final FileLock fileLock = tryLock(channel);
    if (fileLock == null) {
      channel.close();
      throw new IOException("Could not obtain file lock");
    }
    this.channel = channel;
    this.fileLock = fileLock;
    this.version = version;
  }

  private File baseDir(final String checkpointDir, String topic) throws IOException {
    final File dir = new File(checkpointDir, topic);
    if ((!dir.exists() && !dir.mkdirs()) || !dir.isDirectory() || !dir.canWrite()) {
      throw new IOException(
          String.format(
              "Cannot access or write to directory [%s]", dir.getPath()));
    }
    return dir;
  }

  private FileLock tryLock(final FileChannel channel) throws IOException {
    try {
      return channel.tryLock();
    } catch (final OverlappingFileLockException e) {
      return null;
    }
  }

  /**
   * Write the given offsets to the checkpoint file. All offsets should be non-negative.
   *
   * @throws IOException if any file operation fails with an IO exception
   */
  public void write(final Map<TopicPartition, Long> offsets) throws IOException {
    // if the version is negative, skip
    if (version < 0) {
      return;
    }
    // if there is no offsets, skip writing the file to save disk IOs
    if (offsets.isEmpty()) {
      return;
    }

    synchronized (lock) {
      // write to temp file and then swap with the existing file
      final File temp = new File(file.getAbsolutePath() + ".tmp");
      LOG.trace("Writing tmp checkpoint file {}", temp.getAbsolutePath());

      final FileOutputStream fileOutputStream = new FileOutputStream(temp);
      try (final BufferedWriter writer = new BufferedWriter(
          new OutputStreamWriter(fileOutputStream, StandardCharsets.UTF_8))) {
        writeIntLine(writer, version);
        writeIntLine(writer, offsets.size());

        for (final Map.Entry<TopicPartition, Long> entry : offsets.entrySet()) {
          final TopicPartition tp = entry.getKey();
          final Long offset = entry.getValue();
          if (offset >= 0L) {
            writeEntry(writer, tp, offset);
          } else {
            LOG.warn("Received offset={} to write to checkpoint file for {}", offset, tp);
          }
        }

        writer.flush();
        fileOutputStream.getFD().sync();
      }

      LOG.trace("Swapping tmp checkpoint file {} {}", temp.toPath(), file.toPath());
      Utils.atomicMoveWithFallback(temp.toPath(), file.toPath());
    }
  }

  /**
   * @throws IOException if file write operations failed with any IO exception
   */
  static void writeIntLine(
      final BufferedWriter writer,
      final int number
  ) throws IOException {
    writer.write(Integer.toString(number));
    writer.newLine();
  }

  /**
   * @throws IOException if file write operations failed with any IO exception
   */
  static void writeEntry(
      final BufferedWriter writer,
      final TopicPartition part,
      final long offset
  ) throws IOException {
    writer.write(part.topic());
    writer.write(' ');
    writer.write(Integer.toString(part.partition()));
    writer.write(' ');
    writer.write(Long.toString(offset));
    writer.newLine();
  }

  /**
   * Reads the offsets from the local checkpoint file, skipping any negative offsets it finds.
   *
   * @throws IOException              if any file operation fails with an IO exception
   * @throws IllegalArgumentException if the offset checkpoint version is unknown
   */
  public Map<TopicPartition, Long> read() throws IOException {
    synchronized (lock) {
      try (final BufferedReader reader = Files.newBufferedReader(file.toPath())) {
        final int oldVersion = readInt(reader);
        if (oldVersion == version) {
          final int expectedSize = readInt(reader);
          final Map<TopicPartition, Long> offsets = new HashMap<>();
          String line = reader.readLine();
          while (line != null) {
            final String[] pieces = WHITESPACE_MINIMUM_ONCE.split(line);
            if (pieces.length != 3) {
              throw new IOException(
                  String.format("Malformed line in offset checkpoint file: '%s'.", line));
            }

            final String topic = pieces[0];
            final int partition = Integer.parseInt(pieces[1]);
            final TopicPartition tp = new TopicPartition(topic, partition);
            final long offset = Long.parseLong(pieces[2]);
            if (offset >= 0L) {
              offsets.put(tp, offset);
            } else {
              LOG.warn("Read offset={} from checkpoint file for {}", offset, tp);
            }

            line = reader.readLine();
          }
          if (offsets.size() != expectedSize) {
            throw new IOException(
                String.format(
                    "Expected %d entries but found only %d", expectedSize, offsets.size()));
          }
          return offsets;
        } else {
          LOG.warn("Old offset checkpoint version: {}", oldVersion);
        }
      } catch (final NoSuchFileException e) {
        // ignore
      }
      return Collections.emptyMap();
    }
  }

  /**
   * @throws IOException if file read ended prematurely
   */
  static int readInt(final BufferedReader reader) throws IOException {
    final String line = reader.readLine();
    if (line == null) {
      throw new EOFException("File ended prematurely.");
    }
    return Integer.parseInt(line);
  }

  @Override
  public void close() throws IOException {
    if (fileLock == null) {
      return;
    }
    fileLock.release();
    channel.close();
    fileLock = null;
    channel = null;
  }

  /**
   * @throws IOException if there is any IO exception during delete
   */
  public void delete() throws IOException {
    Files.deleteIfExists(file.toPath());
  }

  @Override
  public String toString() {
    return file.getAbsolutePath();
  }

}
