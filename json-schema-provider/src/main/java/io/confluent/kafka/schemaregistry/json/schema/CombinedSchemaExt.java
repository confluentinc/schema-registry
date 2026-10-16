/*
 * Copyright 2024 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.json.schema;

import java.util.Collection;
import java.util.Objects;
import org.everit.json.schema.CombinedSchema;
import org.everit.json.schema.Schema;

/**
 * Validator for {@code allOf}, {@code oneOf}, {@code anyOf} schemas.
 */
public class CombinedSchemaExt extends CombinedSchema {

  /**
   * Builder class for {@link CombinedSchemaExt}.
   */
  public static class Builder extends CombinedSchema.Builder {

    private boolean generated;

    @Override
    public CombinedSchemaExt build() {
      return new CombinedSchemaExt(this);
    }

    public Builder criterion(ValidationCriterion criterion) {
      return (Builder) super.criterion(criterion);
    }

    public Builder subschema(Schema subschema) {
      return (Builder) super.subschema(subschema);
    }

    public Builder subschemas(Collection<Schema> subschemas) {
      return (Builder) super.subschemas(subschemas);
    }

    public Builder isSynthetic(boolean synthetic) {
      return (Builder) super.isSynthetic(synthetic);
    }

    public Builder isGenerated(boolean generated) {
      this.generated = generated;
      return this;
    }
  }

  public static Builder allOf(Collection<Schema> schemas) {
    return builder(schemas).criterion(ALL_CRITERION);
  }

  public static Builder anyOf(Collection<Schema> schemas) {
    return builder(schemas).criterion(ANY_CRITERION);
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder builder(Collection<Schema> subschemas) {
    return new Builder().subschemas(subschemas);
  }

  public static Builder oneOf(Collection<Schema> schemas) {
    return builder(schemas).criterion(ONE_CRITERION);
  }

  private final boolean generated;

  /**
   * Constructor.
   *
   * @param builder the builder containing the validation criterion and the subschemas to be
   *                checked
   */
  public CombinedSchemaExt(Builder builder) {
    super(builder);
    this.generated = builder.generated;
  }

  boolean isGenerated() {
    return generated;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    CombinedSchemaExt that = (CombinedSchemaExt) o;
    return generated == that.generated;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), generated);
  }
}
