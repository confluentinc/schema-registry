/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.utils;

import com.google.common.base.CharMatcher;
import java.util.Objects;

public class QualifiedSubject implements Comparable<QualifiedSubject> {

  public static final String DEFAULT_TENANT = "default";

  public static final String DEFAULT_CONTEXT = ".";

  public static final String TENANT_DELIMITER = "_";

  public static final String CONTEXT_DELIMITER = ":";

  public static final String CONTEXT_SEPARATOR = ".";

  public static final String CONTEXT_PREFIX = CONTEXT_DELIMITER + CONTEXT_SEPARATOR;

  public static final String WILDCARD = "*";

  public static final String CONTEXT_WILDCARD = CONTEXT_DELIMITER + WILDCARD + CONTEXT_DELIMITER;

  // A special context that is used for global configuration and mode settings.
  public static final String GLOBAL_CONTEXT_NAME = ".__GLOBAL";

  // Subject name under which global permissions are stored.
  public static final String GLOBAL_SUBJECT_NAME = "__GLOBAL";

  // Subject name that represents empty string
  public static final String EMPTY_SUBJECT_NAME = "__EMPTY";

  private final String tenant;
  private final String context;  // assumed to start with CONTEXT_SEPARATOR
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
        context = contextSubject.substring(1, ix);  // skip first CONTEXT_DELIMITER
        subject = contextSubject.substring(ix + 1);
      } else {
        context = contextSubject.substring(1);      // skip first CONTEXT_DELIMITER
        subject = "";
      }
    } else if (contextSubject.startsWith(CONTEXT_WILDCARD)) {
      context = WILDCARD;
      subject = contextSubject.substring(CONTEXT_WILDCARD.length());
    } else {
      context = DEFAULT_CONTEXT;
      subject = contextSubject;
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
    return toQualifiedSubject();
  }

  public String toQualifiedContext() {
    String unqualifiedContext = toUnqualifiedContext();
    if (DEFAULT_TENANT.equals(tenant)) {
      return unqualifiedContext;
    } else {
      return tenant + TENANT_DELIMITER + unqualifiedContext;
    }
  }

  /**
   * Returns the context without the tenant prefix.
   */
  public String toUnqualifiedContext() {
    return DEFAULT_CONTEXT.equals(context)
        ? ""
        : CONTEXT_DELIMITER + context + CONTEXT_DELIMITER;
  }

  public String toQualifiedSubject() {
    return toQualifiedContext() + subject;
  }

  /**
   * Returns the context and subject without the tenant prefix.
   */
  public String toUnqualifiedSubject() {
    return toUnqualifiedContext() + subject;
  }

  public static boolean isQualified(String tenant, String subject) {
    return !DEFAULT_TENANT.equals(tenant)
        && subject != null
        && subject.startsWith(tenant + TENANT_DELIMITER);
  }

  public static QualifiedSubject create(String tenant, String qualifiedSubject) {
    try {
      return qualifiedSubject != null ? new QualifiedSubject(tenant, qualifiedSubject) : null;
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  public static QualifiedSubject createFromUnqualified(String tenant, String unqualifiedSubject) {
    try {
      if (unqualifiedSubject == null) {
        return null;
      }
      String qualifiedSubject = DEFAULT_TENANT.equals(tenant)
          ? unqualifiedSubject
          : tenant + TENANT_DELIMITER + unqualifiedSubject;
      return new QualifiedSubject(tenant, qualifiedSubject);
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  public static String contextFor(String tenant, String qualifiedSubject) {
    QualifiedSubject qs = QualifiedSubject.create(tenant, qualifiedSubject);
    return qs != null ? qs.getContext() : DEFAULT_CONTEXT;
  }

  public static String qualifiedContextFor(String tenant, String qualifiedSubject) {
    QualifiedSubject qs = QualifiedSubject.create(tenant, qualifiedSubject);
    return qs != null ? qs.toQualifiedContext() : "";
  }

  /**
   * Checks if the given qualified subject is a context for the given qualified subject.
   *
   * @param tenant the tenant
   * @param qualifiedSubject the qualified subject
   * @param qualifiedContext the qualified context, which must not specify a subject
   * @return true if the subject is in the context, false otherwise
   */
  public static boolean isSubjectInContext(
      String tenant, String qualifiedSubject, QualifiedSubject qualifiedContext) {
    return qualifiedContext != null
        && qualifiedContext.getSubject().isEmpty() // must be a context without a subject
        && (qualifiedContext.getContext().equals(GLOBAL_CONTEXT_NAME)
        || qualifiedContext.toQualifiedContext().equals(
            QualifiedSubject.qualifiedContextFor(tenant, qualifiedSubject)));
  }

  public static QualifiedSubject qualifySubjectWithParent(
      String tenant, String parent, String subjectWithoutTenant) {
    return qualifySubjectWithParent(tenant, parent, subjectWithoutTenant, false);
  }

  public static QualifiedSubject qualifySubjectWithParent(
      String tenant, String parent, String subjectWithoutTenant, boolean prefixTenant) {
    // Since the subject has no tenant, pass the default tenant
    QualifiedSubject qualifiedSubject =
        QualifiedSubject.create(DEFAULT_TENANT, subjectWithoutTenant);
    if (qualifiedSubject == null) {
      return null;
    }
    boolean isQualified = !DEFAULT_CONTEXT.equals(qualifiedSubject.getContext());
    if (!isQualified) {
      QualifiedSubject qualifiedParent = QualifiedSubject.create(tenant, parent);
      boolean isParentQualified = qualifiedParent != null
          && !DEFAULT_CONTEXT.equals(qualifiedParent.getContext());
      if (isParentQualified) {
        // Since the subject has no tenant, pass the default tenant
        qualifiedSubject = new QualifiedSubject(
            DEFAULT_TENANT, qualifiedParent.getContext(), subjectWithoutTenant);
      }
    }
    if (prefixTenant) {
      // Prefix the tenant if prefixTenant is true.
      // For example, references are stored without tenant prefixes,
      // while alias replacements need the tenant.
      qualifiedSubject = new QualifiedSubject(
          tenant, qualifiedSubject.getContext(), qualifiedSubject.getSubject());
    }
    return qualifiedSubject;
  }

  /**
   * Normalizes the given qualified subject name.
   *
   * @param tenant the tenant
   * @param qualifiedSubject the qualified subject name
   * @return the normalized subject name
   */
  public static String normalize(String tenant, String qualifiedSubject) {
    QualifiedSubject qs = QualifiedSubject.create(tenant, qualifiedSubject);
    return qs != null ? qs.toQualifiedSubject() : null;
  }

  public static String normalizeContext(String context) {
    if (context == null) {
      return null;
    }
    if (context.startsWith(CONTEXT_DELIMITER)) {
      context = context.substring(1);
    }
    if (context.endsWith(CONTEXT_DELIMITER)) {
      context = context.substring(0, context.length() - 1);
    }
    if (context.contains(CONTEXT_DELIMITER)) {
      throw new IllegalArgumentException("Context name cannot contain a colon");
    }
    if (!context.startsWith(CONTEXT_SEPARATOR)) {
      context = CONTEXT_SEPARATOR + context;
    }
    return DEFAULT_CONTEXT.equals(context) ? "" : CONTEXT_DELIMITER + context + CONTEXT_DELIMITER;
  }

  public static boolean isDefaultContext(String tenant, String qualifiedSubject) {
    QualifiedSubject qs = QualifiedSubject.create(tenant, qualifiedSubject);
    return qs == null || (qs.getContext().equals(DEFAULT_CONTEXT) && qs.getSubject().isEmpty());
  }

  public static boolean isGlobalContext(String tenant, String qualifiedSubject) {
    QualifiedSubject qs = QualifiedSubject.create(tenant, qualifiedSubject);
    if (qs == null || qs.getContext() == null) {
      return false;
    }
    return qs.getContext().equals(GLOBAL_CONTEXT_NAME)
      && (qs.getSubject().isEmpty() || qs.getSubject() == null);
  }

  public static boolean isValidSubject(String tenant, String qualifiedSubject) {
    return isValidSubject(tenant, qualifiedSubject, false);
  }

  public static boolean isValidSubject(
      String tenant, String qualifiedSubject, boolean isConfigOrMode) {
    if (qualifiedSubject == null || CharMatcher.javaIsoControl().matchesAnyOf(qualifiedSubject)) {
      return false;
    }
    QualifiedSubject qs = QualifiedSubject.create(tenant, qualifiedSubject);
    // For backward compatibility, we allow an empty subject
    if (qs == null || qs.getSubject().equals(GLOBAL_SUBJECT_NAME)
        || qs.getSubject().equals(EMPTY_SUBJECT_NAME)) {
      return false;
    }
    if (!isConfigOrMode && qs.getContext().equals(GLOBAL_CONTEXT_NAME)) {
      // Global context is only valid for config or mode subjects
      return false;
    }
    return true;
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

