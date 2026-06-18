/*
 * Copyright 2026 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.type.logical.playground;

import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.type.logical.LocatedValidationException;
import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.LogicalTypesSchemaVisitor;
import io.confluent.kafka.schemaregistry.type.logical.Schema;
import io.confluent.kafka.schemaregistry.type.logical.ValidationException;
import io.confluent.kafka.schemaregistry.type.logical.avro.LogicalTypeToAvroConverter;
import io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesLexer;
import io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesParser;
import io.confluent.kafka.schemaregistry.type.logical.json.LogicalTypeToJsonConverter;
import io.confluent.kafka.schemaregistry.type.logical.playground.dto.ConvertRequest;
import io.confluent.kafka.schemaregistry.type.logical.playground.dto.ConvertResponse;
import io.confluent.kafka.schemaregistry.type.logical.protobuf.LogicalTypeToProtoConverter;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.NoViableAltException;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.misc.Interval;

@ApplicationScoped
public class ConversionService {

  /**
   * Single-document entry point — preserved for callers that haven't migrated.
   */
  public ConvertResponse convert(String sql, String target, String rowName) {
    ConvertRequest req = new ConvertRequest();
    req.sql = sql;
    req.target = target;
    req.rowName = rowName;
    return convert(req);
  }

  public ConvertResponse convert(ConvertRequest req) {
    ConvertResponse resp = new ConvertResponse();
    resp.errors = new ArrayList<>();
    String row = (req.rowName == null || req.rowName.isBlank()) ? "Row" : req.rowName;
    String tgt = (req.target == null || req.target.isBlank()) ? "avro" : req.target.toLowerCase();

    if (req.documents != null && !req.documents.isEmpty()) {
      return convertMulti(req.documents, req.activeDocument, tgt, row, resp);
    }
    if (req.sql == null || req.sql.isBlank()) {
      return resp;
    }
    return convertSingle(req.sql, tgt, row, resp);
  }

  /**
   * Convert {@code activeDocument}, resolving any inferred external reference against
   * other documents. Documents are indexed by the qualified type names they
   * <em>define</em> (not by tab name), so external lookup works regardless of how
   * tabs are labeled.
   */
  private ConvertResponse convertMulti(
      List<ConvertRequest.Document> documents,
      String activeName,
      String target,
      String rowName,
      ConvertResponse resp) {
    ConvertRequest.Document active = null;
    Map<String, ParsedDoc> byDefinedType = new LinkedHashMap<>();
    for (ConvertRequest.Document d : documents) {
      if (d == null || d.name == null) {
        continue;
      }
      if (d.name.equals(activeName)) {
        active = d;
        // Don't pre-parse the active doc here — we want its errors surfaced via
        // resp.errors below, not silently skipped.
        continue;
      }
      if (d.ddl == null || d.ddl.isBlank()) {
        continue;
      }
      // Pre-parse non-active docs into the lookup index. Errored docs are skipped;
      // they'll surface as "Document defining 'X' has parse errors" if referenced.
      List<ConvertResponse.Marker> ignored = new ArrayList<>();
      ParsedDoc p = parse(d.ddl, ignored);
      if (p == null || !ignored.isEmpty()) {
        continue;
      }
      for (String defined : p.namedTypes.keySet()) {
        byDefinedType.put(defined, p);
      }
    }
    if (active == null) {
      resp.errors.add(new ConvertResponse.Marker(1, 0, 1, 0,
          "Active document '" + activeName + "' is not in the document set"));
      return resp;
    }
    if (active.ddl == null || active.ddl.isBlank()) {
      return resp;
    }

    ParsedDoc parsed = parse(active.ddl, resp.errors);
    if (parsed == null || !resp.errors.isEmpty()) {
      return resp;
    }

    try {
      Map<String, String> resolvedRefs = new LinkedHashMap<>();
      List<SchemaReference> refs = new ArrayList<>();
      resolveReferences(parsed, byDefinedType, target, rowName, new HashSet<>(),
          new LinkedHashMap<>(), resolvedRefs, refs);

      LogicalType activeLt = withReferences(parsed, refs, resolvedRefs);
      resp.schema = render(activeLt, target, rowName);
    } catch (ValidationException e) {
      resp.errors.add(toMarker(e, e.getMessage()));
    } catch (RuntimeException e) {
      resp.errors.add(toMarker(e,
          e.getClass().getSimpleName() + ": " + e.getMessage()));
    }
    return resp;
  }

  private ConvertResponse convertSingle(
      String sql, String target, String rowName, ConvertResponse resp) {
    ParsedDoc parsed = parse(sql, resp.errors);
    if (parsed == null || !resp.errors.isEmpty()) {
      return resp;
    }
    try {
      resp.schema = render(parsed.toLogicalType(), target, rowName);
    } catch (ValidationException e) {
      resp.errors.add(toMarker(e, e.getMessage()));
    } catch (RuntimeException e) {
      resp.errors.add(toMarker(e,
          e.getClass().getSimpleName() + ": " + e.getMessage()));
    }
    return resp;
  }

  /**
   * Recursively resolve every external reference of {@code doc}, converting each
   * dependency to {@code target} and recording it under its qualified name. Externals
   * are inferred from usage (any NAMED_TYPE_REF FQN not declared locally in the doc).
   * The lookup matches a reference name against any document that <em>defines</em>
   * that name in its {@code namedTypes}. Handles transitive references and cycles
   * via {@code visiting}.
   */
  private void resolveReferences(
      ParsedDoc doc,
      Map<String, ParsedDoc> byDefinedType,
      String target,
      String rowName,
      Set<String> visiting,
      Map<String, String> renderedCache,
      Map<String, String> outResolvedRefs,
      List<SchemaReference> outRefs) {
    for (String refName : doc.referencedTypes) {
      if (renderedCache.containsKey(refName)) {
        outResolvedRefs.put(refName, renderedCache.get(refName));
        outRefs.add(new SchemaReference(refName, refName, 1));
        continue;
      }
      if (visiting.contains(refName)) {
        throw new ValidationException(
            "Cyclic reference chain involving '" + refName + "'");
      }
      ParsedDoc dep = byDefinedType.get(refName);
      if (dep == null) {
        throw new ValidationException(
            "Referenced type '" + refName + "' has no defining document in the workspace");
      }
      visiting.add(refName);
      // Depth-first: resolve transitive refs of the dep before rendering it.
      Map<String, String> transitiveResolved = new LinkedHashMap<>();
      List<SchemaReference> transitiveRefs = new ArrayList<>();
      resolveReferences(dep, byDefinedType, target, rowName, visiting, renderedCache,
          transitiveResolved, transitiveRefs);
      // Render the named type itself as the root, not the dep's rootSchema.
      // The dep's rootSchema may be a *nullable* NAMED_TYPE_REF (e.g. a
      // bare `STRUCT T (...)` defaults to nullable), which would emit Avro as
      // ["null", record]. A union can't carry the record's name through
      // AvroSchema.canonicalString()'s `referencedSchemas` dedup, so the
      // active doc would inline the record. Rendering the named type directly
      // produces a clean top-level record/enum.
      LogicalType depLt = depAsReferencedType(dep, refName, transitiveRefs, transitiveResolved);
      String depRendered = render(depLt, target, refName);
      visiting.remove(refName);
      renderedCache.put(refName, depRendered);
      outResolvedRefs.put(refName, depRendered);
      outRefs.add(new SchemaReference(refName, refName, 1));
    }
  }

  /**
   * Build a LogicalType from a parsed document plus a resolved reference set. The
   * visitor's own {@link LogicalTypesSchemaVisitor#toLogicalType()} doesn't accept
   * references, so we reconstruct via the full constructor.
   */
  private static LogicalType withReferences(
      ParsedDoc doc,
      List<SchemaReference> refs,
      Map<String, String> resolvedRefs) {
    return new LogicalType(
        doc.namespace,
        doc.rootSchema,
        doc.namedTypes,
        refs,
        resolvedRefs);
  }

  /**
   * Build a LogicalType that emits {@code refName} as the root named type — used
   * when rendering a dep so its canonical output is just the named record/enum
   * (not the dep's rootSchema, which may be a nullable wrapper).
   */
  private static LogicalType depAsReferencedType(
      ParsedDoc dep,
      String refName,
      List<SchemaReference> refs,
      Map<String, String> resolvedRefs) {
    Schema rootRef = Schema.createNamedTypeRef(refName).setNullable(false);
    return new LogicalType(
        dep.namespace,
        rootRef,
        dep.namedTypes,
        refs,
        resolvedRefs);
  }

  /**
   * Lex + parse + visit a single DDL string. Lexer/parser errors are appended to
   * {@code errors}. Returns null on hard failure (e.g., unrecoverable parse error).
   */
  private static ParsedDoc parse(String ddl, List<ConvertResponse.Marker> errors) {
    LogicalTypesLexer lexer = new LogicalTypesLexer(CharStreams.fromString(ddl));
    CollectingErrorListener listener = new CollectingErrorListener(errors);
    lexer.removeErrorListeners();
    lexer.addErrorListener(listener);

    LogicalTypesParser parser = new LogicalTypesParser(new CommonTokenStream(lexer));
    parser.removeErrorListeners();
    parser.addErrorListener(listener);

    LogicalTypesParser.ScriptContext tree;
    try {
      tree = parser.script();
    } catch (RecognitionException e) {
      return null;
    }
    if (!errors.isEmpty()) {
      return null;
    }
    LogicalTypesSchemaVisitor visitor = new LogicalTypesSchemaVisitor();
    try {
      visitor.visit(tree);
    } catch (ValidationException e) {
      errors.add(toMarker(e, e.getMessage()));
      return null;
    } catch (RuntimeException e) {
      errors.add(toMarker(e, e.getClass().getSimpleName() + ": " + e.getMessage()));
      return null;
    }
    return new ParsedDoc(
        visitor.getNamespace(),
        visitor.getRootSchema(),
        visitor.getNamedTypes(),
        inferReferencedTypes(visitor.getRootSchema(), visitor.getNamedTypes()));
  }

  /**
   * Mirror of {@code LogicalTypesSchemaVisitor#inferExternalTypes()}: walk the root +
   * every local body, collect every NAMED_TYPE_REF FQN, subtract locally-declared
   * names. Returns the inferred external set in insertion order.
   */
  private static List<String> inferReferencedTypes(
      Schema rootSchema, Map<String, Schema> namedTypes) {
    Set<String> refs = new java.util.LinkedHashSet<>();
    if (rootSchema != null) {
      LogicalType.collectNamedRefs(rootSchema, refs);
    }
    for (Schema body : namedTypes.values()) {
      LogicalType.collectNamedRefs(body, refs);
    }
    refs.removeAll(namedTypes.keySet());
    return new ArrayList<>(refs);
  }

  /**
   * Build a marker pointing at the source position carried by a
   * {@link LocatedValidationException}, or fall back to (1, 0) for exceptions
   * without position info (legacy validation throws or unexpected runtime errors).
   */
  private static ConvertResponse.Marker toMarker(Throwable e, String message) {
    if (e instanceof LocatedValidationException loc) {
      return new ConvertResponse.Marker(
          loc.getLine(), loc.getColumn(),
          loc.getEndLine(), loc.getEndColumn(),
          message);
    }
    return new ConvertResponse.Marker(1, 0, 1, 0, message);
  }

  private String render(LogicalType logicalType, String target, String rowName) {
    switch (target) {
      case "avro":
        return LogicalTypeToAvroConverter.fromLogicalType(logicalType, rowName)
            .canonicalString();
      case "protobuf":
        return LogicalTypeToProtoConverter.fromLogicalType(logicalType, rowName)
            .canonicalString();
      case "json":
        return LogicalTypeToJsonConverter.fromLogicalType(logicalType, rowName)
            .canonicalString();
      default:
        throw new IllegalArgumentException(
            "Unknown target: " + target + " (expected avro|protobuf|json)");
    }
  }

  /** Visitor outputs bundled for re-construction with references plugged in. */
  private static final class ParsedDoc {
    final String namespace;
    final Schema rootSchema;
    final Map<String, Schema> namedTypes;
    final List<String> referencedTypes;

    ParsedDoc(String namespace, Schema rootSchema,
              Map<String, Schema> namedTypes, List<String> referencedTypes) {
      this.namespace = namespace;
      this.rootSchema = rootSchema;
      this.namedTypes = namedTypes;
      this.referencedTypes = referencedTypes;
    }

    LogicalType toLogicalType() {
      return new LogicalType(
          namespace, rootSchema, namedTypes,
          java.util.Collections.emptyList(), java.util.Collections.emptyMap());
    }
  }

  private static final class CollectingErrorListener extends BaseErrorListener {
    private final List<ConvertResponse.Marker> sink;

    CollectingErrorListener(List<ConvertResponse.Marker> sink) {
      this.sink = sink;
    }

    @Override
    public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol,
                            int line, int charPositionInLine, String msg,
                            RecognitionException e) {
      int len = 1;
      if (offendingSymbol instanceof Token tok && tok.getText() != null) {
        len = Math.max(1, tok.getText().length());
      }
      sink.add(new ConvertResponse.Marker(
          line, charPositionInLine, line, charPositionInLine + len,
          rewriteMessage(recognizer, msg, e)));
    }

    /**
     * ANTLR's {@link NoViableAltException} message embeds the offending input as
     * a concatenation of token text — so {@code "STRUCT MyRecord ("} renders as
     * {@code "ROWxMyRecordx("}. Rebuild the quoted span by extracting the
     * original characters (including whitespace) from the underlying input.
     */
    private static String rewriteMessage(
        Recognizer<?, ?> recognizer, String msg, RecognitionException e) {
      if (!(e instanceof NoViableAltException nva)) {
        return msg;
      }
      Token start = nva.getStartToken();
      Token offending = nva.getOffendingToken();
      if (start == null || offending == null
          || !(recognizer.getInputStream() instanceof TokenStream ts)) {
        return msg;
      }
      CharStream cs = ts.getTokenSource().getInputStream();
      try {
        String original = cs.getText(
            Interval.of(start.getStartIndex(), offending.getStopIndex()));
        return "no viable alternative at input '" + original + "'";
      } catch (RuntimeException ignored) {
        return msg;
      }
    }
  }
}
