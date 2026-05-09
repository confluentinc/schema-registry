/*
 * Copyright 2026 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.rules.cel.builtin;

import io.confluent.kafka.schemaregistry.type.Variant;
import java.util.ArrayList;
import java.util.List;

/**
 * JSONPath subset used by {@code variant_get(v, path)}. Supports:
 *
 * <ul>
 *   <li>{@code $} — root</li>
 *   <li>{@code $.field} — object field by identifier name</li>
 *   <li>{@code $.field.subfield} — nested fields</li>
 *   <li>{@code $[i]} — array element by index</li>
 *   <li>{@code $.field[i].sub} — mixed</li>
 *   <li>{@code $["foo bar"]} — quoted key for non-identifier names</li>
 * </ul>
 *
 * <p>Path-resolution failures (missing field, out-of-bounds index, type mismatch)
 * return {@code null} from {@link #walk}. Malformed paths throw
 * {@link IllegalArgumentException} at parse time.
 *
 * <p>Identifier names follow {@code [A-Za-z_][A-Za-z0-9_]*}. Use the quoted form
 * for any key with characters outside that set.
 */
final class VariantPath {

  private VariantPath() {
  }

  /**
   * Walk {@code root} following {@code path}. Returns the resolved Variant, or
   * {@code null} if any segment fails to resolve. Throws on malformed path.
   */
  static Variant walk(Variant root, String path) {
    List<Segment> segments = parse(path);
    Variant current = root;
    for (Segment seg : segments) {
      if (current == null) {
        return null;
      }
      current = seg.apply(current);
    }
    return current;
  }

  /**
   * Parse {@code path} into a list of segments. Visible for testing.
   */
  static List<Segment> parse(String path) {
    if (path == null || path.isEmpty()) {
      throw new IllegalArgumentException("variant path must start with '$'");
    }
    Cursor c = new Cursor(path);
    if (c.peek() != '$') {
      throw new IllegalArgumentException(
          "variant path must start with '$', got: " + path);
    }
    c.next();
    List<Segment> out = new ArrayList<>();
    while (c.hasMore()) {
      char ch = c.peek();
      if (ch == '.') {
        c.next();
        out.add(new FieldSegment(readIdent(c, path)));
      } else if (ch == '[') {
        c.next();
        if (c.peek() == '"' || c.peek() == '\'') {
          out.add(new FieldSegment(readQuotedKey(c, path)));
        } else {
          out.add(new IndexSegment(readIndex(c, path)));
        }
        if (!c.hasMore() || c.next() != ']') {
          throw new IllegalArgumentException(
              "expected ']' in variant path: " + path);
        }
      } else {
        throw new IllegalArgumentException(
            "unexpected character '" + ch + "' in variant path: " + path);
      }
    }
    return out;
  }

  private static String readIdent(Cursor c, String path) {
    int start = c.pos;
    while (c.hasMore()) {
      char ch = c.peek();
      if (Character.isLetterOrDigit(ch) || ch == '_') {
        c.next();
      } else {
        break;
      }
    }
    if (c.pos == start) {
      throw new IllegalArgumentException(
          "expected identifier after '.' in variant path: " + path);
    }
    return c.src.substring(start, c.pos);
  }

  private static String readQuotedKey(Cursor c, String path) {
    char quote = c.next();
    StringBuilder sb = new StringBuilder();
    while (c.hasMore()) {
      char ch = c.next();
      if (ch == '\\' && c.hasMore()) {
        sb.append(c.next());
      } else if (ch == quote) {
        return sb.toString();
      } else {
        sb.append(ch);
      }
    }
    throw new IllegalArgumentException(
        "unterminated quoted key in variant path: " + path);
  }

  private static int readIndex(Cursor c, String path) {
    int start = c.pos;
    if (c.hasMore() && c.peek() == '-') {
      c.next();
    }
    while (c.hasMore() && Character.isDigit(c.peek())) {
      c.next();
    }
    if (c.pos == start) {
      throw new IllegalArgumentException(
          "expected integer index in variant path: " + path);
    }
    try {
      return Integer.parseInt(c.src.substring(start, c.pos));
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          "invalid integer index in variant path: " + path, e);
    }
  }

  interface Segment {
    Variant apply(Variant current);
  }

  static final class FieldSegment implements Segment {
    final String key;

    FieldSegment(String key) {
      this.key = key;
    }

    @Override
    public Variant apply(Variant current) {
      if (current.getType() != Variant.Type.OBJECT) {
        return null;
      }
      return current.getFieldByKey(key);
    }
  }

  static final class IndexSegment implements Segment {
    final int index;

    IndexSegment(int index) {
      this.index = index;
    }

    @Override
    public Variant apply(Variant current) {
      if (current.getType() != Variant.Type.ARRAY) {
        return null;
      }
      return current.getElementAtIndex(index);
    }
  }

  private static final class Cursor {
    final String src;
    int pos;

    Cursor(String src) {
      this.src = src;
    }

    boolean hasMore() {
      return pos < src.length();
    }

    char peek() {
      return src.charAt(pos);
    }

    char next() {
      return src.charAt(pos++);
    }
  }
}
