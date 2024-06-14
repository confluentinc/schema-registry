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

package io.confluent.dekregistry.web.rest.handlers;

import static io.confluent.dekregistry.storage.DekRegistry.AWS_KMS;

import io.confluent.kafka.schemaregistry.client.rest.entities.Rule;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.ConfigUpdateRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.TagSchemaRequest;
import io.confluent.kafka.schemaregistry.rest.handlers.UpdateRequestHandler;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EncryptionUpdateRequestHandler implements UpdateRequestHandler {

  private static final String ENCRYPT_KEK_NAME = "encrypt.kek.name";
  private static final String ENCRYPT_KMS_KEY_ID = "encrypt.kms.key.id";
  private static final String ENCRYPT_KMS_TYPE = "encrypt.kms.type";

  public EncryptionUpdateRequestHandler() {
  }

  @Override
  public void handle(String subject, ConfigUpdateRequest request) {
  }

  @Override
  public void handle(String subject, boolean normalize, RegisterSchemaRequest request) {
    addKmsDefaults(request);
  }

  @Override
  public void handle(Schema schema, TagSchemaRequest request) {
    addKmsDefaults(request);
  }

  private void addKmsDefaults(RegisterSchemaRequest request) {
    request.setRuleSet(addKmsDefaults(request.getRuleSet()));
  }

  private void addKmsDefaults(TagSchemaRequest request) {
    request.setRuleSet(addKmsDefaults(request.getRuleSet()));
  }

  // For now, we generate a KEK name based on the KMS key id.
  // In the future, we may require the KEK name to be explicitly passed.
  private RuleSet addKmsDefaults(RuleSet ruleSet) {
    if (ruleSet == null || ruleSet.getDomainRules() == null) {
      return ruleSet;
    }
    List<Rule> domainRules = ruleSet.getDomainRules();
    List<Rule> newDomainRules = new ArrayList<>();
    for (Rule rule : domainRules) {
      Map<String, String> params = rule.getParams();
      if (params == null || !params.containsKey(ENCRYPT_KMS_KEY_ID)) {
        newDomainRules.add(rule);
        continue;
      }
      if (params.containsKey(ENCRYPT_KEK_NAME) && params.containsKey(ENCRYPT_KMS_TYPE)) {
        newDomainRules.add(rule);
        continue;
      }
      String kmsKeyId = params.get(ENCRYPT_KMS_KEY_ID);
      Map<String, String> newParams = new HashMap<>(params);
      if (!params.containsKey(ENCRYPT_KEK_NAME)) {
        newParams.put(ENCRYPT_KEK_NAME, scrub(kmsKeyId));
      }
      if (!params.containsKey(ENCRYPT_KMS_TYPE) && kmsKeyId.startsWith("arn:")) {
        newParams.put(ENCRYPT_KMS_TYPE, AWS_KMS);
      }
      Rule newRule = new Rule(rule.getName(), rule.getDoc(), rule.getKind(), rule.getMode(),
          rule.getType(), rule.getTags(), newParams, rule.getExpr(),
          rule.getOnSuccess(), rule.getOnFailure(), rule.isDisabled());
      newDomainRules.add(newRule);
    }
    return new RuleSet(ruleSet.getMigrationRules(), newDomainRules);
  }

  private static String scrub(String kmsKeyId) {
    String keyId = kmsKeyId.startsWith("arn:")
        ? kmsKeyId.substring(4)
        : kmsKeyId;
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < keyId.length(); i++) {
      char c = keyId.charAt(i);
      if (Character.isLetterOrDigit(c) || c == '_' || c == '-') {
        sb.append(c);
      } else {
        sb.append("-");
      }
    }
    return sb.toString();
  }
}
