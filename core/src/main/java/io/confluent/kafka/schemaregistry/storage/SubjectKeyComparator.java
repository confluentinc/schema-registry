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

package io.confluent.kafka.schemaregistry.storage;

import io.confluent.kafka.schemaregistry.utils.QualifiedSubject;

import java.util.Comparator;

public class SubjectKeyComparator<K> implements Comparator<K> {

  private final LookupCache<K, ?> lookupCache;

  public SubjectKeyComparator() {
    this.lookupCache = null;
  }

  public SubjectKeyComparator(LookupCache<K, ?> lookupCache) {
    this.lookupCache = lookupCache;
  }

  @Override
  @SuppressWarnings("unchecked")
  public int compare(K o1, K o2) {
    if (o1 instanceof SubjectKey && o2 instanceof SubjectKey) {
      SubjectKey s1 = (SubjectKey) o1;
      SubjectKey s2 = (SubjectKey) o2;
      int cmp = s1.keyType.compareTo(s2.keyType);
      if (cmp != 0) {
        return cmp;
      }
      // If the lookup cache is null, the tenant will be derived from the subject
      String tenant = lookupCache != null ? lookupCache.tenant() : null;
      QualifiedSubject qs1 = QualifiedSubject.create(tenant, s1.getSubject());
      QualifiedSubject qs2 = QualifiedSubject.create(tenant, s2.getSubject());
      if (qs1 == null && qs2 == null) {
        return 0;
      } else if (qs1 == null) {
        return -1;
      } else if (qs2 == null) {
        return 1;
      } else {
        cmp = qs1.compareTo(qs2);
        if (cmp != 0) {
          return cmp;
        }
        if (s1 instanceof SchemaKey && s2 instanceof SchemaKey) {
          SchemaKey sk1 = (SchemaKey) o1;
          SchemaKey sk2 = (SchemaKey) o2;
          return sk1.getVersion() - sk2.getVersion();
        } else {
          return 0;
        }
      }
    } else {
      return ((Comparable) o1).compareTo(o2);
    }
  }
}
