/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.utils;

import java.util.Objects;

public class QualifiedSubject implements Comparable<QualifiedSubject> {

  public static final String DEFAULT_TENANT = "default";

  public static final String DEFAULT_CONTEXT = ".";

  public static final char TENANT_DELIMITER = '_';

  public static final char CONTEXT_DELIMITER = ':';

  public static final String CONTEXT_PREFIX = CONTEXT_DELIMITER + ".";

  private static final String WILDCARD = "*";

  private final String tenant;
  private final String context;
  private final String subject;

  // visible for testing
  protected QualifiedSubject(String tenant, String qualifiedSubject) {
    String contextSubject = qualifiedSubject != null ? qualifiedSubject : "";
    if (!DEFAULT_TENANT.equals(tenant)) {
      int ix = contextSubject.indexOf(TENANT_DELIMITER);
      if (ix >= 0) {
        String tenantPrefix = contextSubject.substring(0, ix);
        contextSubject = contextSubject.substring(ix + 1);
        // set actual tenant, we only really care whether the passed in tenant was "default"
        tenant = tenantPrefix;
      } else {
        tenant = DEFAULT_TENANT;
      }
    }

    String context;
    String subject;
    if (contextSubject.startsWith(CONTEXT_PREFIX)) {
      int ix = contextSubject.substring(CONTEXT_PREFIX.length()).indexOf(CONTEXT_DELIMITER);
      if (ix >= 0) {
        ix += CONTEXT_PREFIX.length();
        context = contextSubject.substring(1, ix);  // skip CONTEXT_DELIMITER
        subject = contextSubject.substring(ix + 1);
      } else {
        context = contextSubject.substring(1);      // skip CONTEXT_DELIMITER
        subject = "";
      }
    } else {
      context = DEFAULT_CONTEXT;
      // check for wildcard for backward compatibility
      subject = WILDCARD.equals(contextSubject) ? "" : contextSubject;
    }

    this.tenant = tenant;
    this.context = context;
    this.subject = subject;
  }

  public QualifiedSubject(String tenant, String context, String subject) {
    this.tenant = tenant != null ? tenant : DEFAULT_TENANT;
    this.context = context != null ? context : DEFAULT_CONTEXT;
    this.subject = subject != null ? subject : "";
  }

  public String getTenant() {
    return tenant;
  }

  public String getContext() {
    return context;
  }

  public String getSubject() {
    return subject;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    QualifiedSubject that = (QualifiedSubject) o;
    return Objects.equals(tenant, that.tenant)
        && Objects.equals(context, that.context)
        && Objects.equals(subject, that.subject);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tenant, context, subject);
  }

  @Override
  public String toString() {
    String qualifiedContext = toQualifiedContext();
    if (DEFAULT_TENANT.equals(tenant)) {
      return qualifiedContext;
    } else {
      return tenant + TENANT_DELIMITER + qualifiedContext;
    }
  }

  public String toQualifiedContext() {
    if (DEFAULT_CONTEXT.equals(context)) {
      return subject;
    } else {
      return CONTEXT_DELIMITER + context + CONTEXT_DELIMITER + subject;
    }
  }

  public String toQualifiedSubject() {
    return toString();
  }

  public static QualifiedSubject create(String tenant, String qualifiedSubject) {
    try {
      return qualifiedSubject != null ? new QualifiedSubject(tenant, qualifiedSubject) : null;
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  public static String contextFor(String tenant, String qualifiedSubject) {
    QualifiedSubject qs = QualifiedSubject.create(tenant, qualifiedSubject);
    return qs != null ? qs.getContext() : DEFAULT_CONTEXT;
  }

  @Override
  public int compareTo(QualifiedSubject that) {
    if (this.getTenant() == null && that.getTenant() == null) {
      // pass
    } else if (this.getTenant() == null) {
      return -1;
    } else if (that.getTenant() == null) {
      return 1;
    } else {
      int tenantComparison = this.getTenant().compareTo(that.getTenant());
      if (tenantComparison != 0) {
        return tenantComparison < 0 ? -1 : 1;
      }
    }

    if (this.getContext() == null && that.getContext() == null) {
      // pass
    } else if (this.getContext() == null) {
      return -1;
    } else if (that.getContext() == null) {
      return 1;
    } else {
      int contextComparison = this.getContext().compareTo(that.getContext());
      if (contextComparison != 0) {
        return contextComparison < 0 ? -1 : 1;
      }
    }

    if (this.getSubject() == null && that.getSubject() == null) {
      return 0;
    } else if (this.getSubject() == null) {
      return -1;
    } else if (that.getSubject() == null) {
      return 1;
    } else {
      return this.getSubject().compareTo(that.getSubject());
    }
  }
}

