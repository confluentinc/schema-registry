/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.rest;

import com.google.common.collect.ImmutableList;
import io.confluent.kafka.schemaregistry.CompatibilityLevel;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.RestApp;
import io.confluent.kafka.schemaregistry.avro.AvroUtils;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.entities.Config;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.Rule;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleKind;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleMode;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import io.confluent.kafka.schemaregistry.client.rest.entities.ExtendedSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.ConfigUpdateRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaResponse;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.rest.exceptions.Errors;
import io.confluent.kafka.schemaregistry.rest.exceptions.RestIncompatibleSchemaException;
import io.confluent.kafka.schemaregistry.rest.exceptions.RestInvalidCompatibilityException;
import io.confluent.kafka.schemaregistry.rest.exceptions.RestInvalidRuleSetException;
import io.confluent.kafka.schemaregistry.rest.exceptions.RestInvalidSchemaException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.apache.avro.SchemaCompatibility.SchemaIncompatibilityType.READER_FIELD_MISSING_DEFAULT_VALUE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@Tag("IntegrationTest")
public abstract class RestApiCompatibilityTest {

  protected RestApp restApp = null;

  public void setRestApp(RestApp restApp) {
    this.restApp = restApp;
  }

  protected int expectedSchemaId(int sequentialId) {
    return sequentialId;
  }

  @Test
  public void testCompatibility() throws Exception {
    String subject = "testSubject";

    // register a valid avro
    String schemaString1 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"}]}").canonicalString();
    int expectedIdSchema1 = expectedSchemaId(1);
    assertEquals(
        expectedIdSchema1,
        restApp.restClient.registerSchema(schemaString1, subject),
        "Registering should succeed"
    );

    // register an incompatible avro
    String incompatibleSchemaString = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"},"
        + " {\"type\":\"string\",\"name\":\"f2\"}]}").canonicalString();
    try {
      restApp.restClient.registerSchema(incompatibleSchemaString, subject);
      fail("Registering an incompatible schema should fail");
    } catch (RestClientException e) {
      // this is expected.
      assertEquals(
          RestIncompatibleSchemaException.DEFAULT_ERROR_CODE,
          e.getStatus(),
          "Should get a conflict status"
      );
      assertTrue(
          e.getMessage().contains(READER_FIELD_MISSING_DEFAULT_VALUE.toString()),
          "Verifying error message verbosity"
      );
    }

    // register a non-avro
    String nonAvroSchemaString = "non-avro schema string";
    try {
      restApp.restClient.registerSchema(nonAvroSchemaString, subject);
      fail("Registering a non-avro schema should fail");
    } catch (RestClientException e) {
      // this is expected.
      assertEquals(
          RestInvalidSchemaException.ERROR_CODE,
          e.getErrorCode(),
          "Should get a bad request status"
      );
    }

    // register a backward compatible avro
    String schemaString2 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"},"
        + " {\"type\":\"string\",\"name\":\"f2\", \"default\": \"foo\"}]}").canonicalString();
    int expectedIdSchema2 = expectedSchemaId(2);
    assertEquals(
        expectedIdSchema2,
        restApp.restClient.registerSchema(schemaString2, subject),
        "Registering a compatible schema should succeed"
    );
  }

  @Test
  public void testForceSkipsCompatibilityCheck() throws Exception {
    String subject = "testForceSubject";

    String schemaString1 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"}]}").canonicalString();
    restApp.restClient.registerSchema(schemaString1, subject);

    // An added required field is not backward-compatible.
    String incompatibleSchemaString = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"},"
        + " {\"type\":\"string\",\"name\":\"f2\"}]}").canonicalString();

    // Without force, registration is rejected by the compatibility check.
    try {
      restApp.restClient.registerSchema(incompatibleSchemaString, subject);
      fail("Registering an incompatible schema should fail without force");
    } catch (RestClientException e) {
      assertEquals(
          RestIncompatibleSchemaException.DEFAULT_ERROR_CODE, e.getStatus(),
          "Should get a conflict status");
    }

    // With force=true, the compatibility check is skipped and the schema registers.
    RegisterSchemaRequest request = new RegisterSchemaRequest();
    request.setSchema(incompatibleSchemaString);
    RegisterSchemaResponse response = restApp.restClient.registerSchema(
        RestService.DEFAULT_REQUEST_PROPERTIES, request, subject, false, true, null);
    assertTrue(response.getId() > 0, "Forced registration of an incompatible schema should succeed");
  }

  @Test
  public void testCompatibilityLevelChangeToNone() throws Exception {
    String subject = "testSubject";

    // register a valid avro
    String schemaString1 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"}]}").canonicalString();
    int expectedIdSchema1 = expectedSchemaId(1);
    assertEquals(
        expectedIdSchema1,
        restApp.restClient.registerSchema(schemaString1, subject),
        "Registering should succeed"
    );

    // register an incompatible avro
    String incompatibleSchemaString = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"},"
        + " {\"type\":\"string\",\"name\":\"f2\"}]}").canonicalString();
    try {
      restApp.restClient.registerSchema(incompatibleSchemaString, subject);
      fail("Registering an incompatible schema should fail");
    } catch (RestClientException e) {
      // this is expected.
      assertEquals(
          RestIncompatibleSchemaException.DEFAULT_ERROR_CODE,
          e.getStatus(),
          "Should get a conflict status"
      );
    }

    // change compatibility level to none and try again
    assertEquals(
        CompatibilityLevel.NONE.name,
        restApp.restClient
            .updateCompatibility(CompatibilityLevel.NONE.name, null)
            .getCompatibilityLevel(),
        "Changing compatibility level should succeed"
    );

    try {
      restApp.restClient.registerSchema(incompatibleSchemaString, subject);
    } catch (RestClientException e) {
      fail("Registering an incompatible schema should succeed after bumping down the compatibility "
           + "level to none");
    }
  }

  @Test
  public void testCompatibilityLevelChangeToBackward() throws Exception {
    String subject = "testSubject";

    String schemaString1 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"}]}").canonicalString();
    int expectedIdSchema1 = expectedSchemaId(1);
    assertEquals(
        expectedIdSchema1,
        restApp.restClient.registerSchema(schemaString1, subject),
        "Registering should succeed"
    );
    // verify that default compatibility level is backward
    assertEquals(
        new Config(CompatibilityLevel.BACKWARD.name),
        restApp.restClient.getConfig(null),
        "Default compatibility level should be backward"
    );
    // change it to forward
    assertEquals(
        CompatibilityLevel.FORWARD.name,
        restApp.restClient
            .updateCompatibility(CompatibilityLevel.FORWARD.name, null)
            .getCompatibilityLevel(),
        "Changing compatibility level should succeed"
    );

    // verify that new compatibility level is forward
    assertEquals(
        new Config(CompatibilityLevel.FORWARD.name),
        restApp.restClient.getConfig(null),
        "New compatibility level should be forward"
    );

    // register schema that is forward compatible with schemaString1
    String schemaString2 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"},"
        + " {\"type\":\"string\",\"name\":\"f2\"}]}").canonicalString();
    int expectedIdSchema2 = expectedSchemaId(2);
    assertEquals(
        expectedIdSchema2,
        restApp.restClient.registerSchema(schemaString2, subject),
        "Registering should succeed"
    );

    // change compatibility to backward
    assertEquals(
         CompatibilityLevel.BACKWARD.name,
         restApp.restClient.updateCompatibility(CompatibilityLevel.BACKWARD.name,
             null).getCompatibilityLevel(),
        "Changing compatibility level should succeed"
    );

    // verify that new compatibility level is backward
    assertEquals(
        new Config(CompatibilityLevel.BACKWARD.name),
        restApp.restClient.getConfig(null),
        "Updated compatibility level should be backward"
    );

    // register forward compatible schema, which should fail
    String schemaString3 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"},"
        + " {\"type\":\"string\",\"name\":\"f2\"},"
        + " {\"type\":\"string\",\"name\":\"f3\"}]}").canonicalString();
    try {
      restApp.restClient.registerSchema(schemaString3, subject);
      fail("Registering a forward compatible schema should fail");
    } catch (RestClientException e) {
      // this is expected.
      assertEquals(
          RestIncompatibleSchemaException.DEFAULT_ERROR_CODE,
          e.getStatus(),
          "Should get a conflict status"
      );
    }

    // now try registering a backward compatible schema (add a field with a default)
    String schemaString4 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"},"
        + " {\"type\":\"string\",\"name\":\"f2\"},"
        + " {\"type\":\"string\",\"name\":\"f3\", \"default\": \"foo\"}]}").canonicalString();
    int expectedIdSchema4 = expectedSchemaId(3);
    assertEquals(
        expectedIdSchema4,
        restApp.restClient.registerSchema(schemaString4, subject),
        "Registering should succeed with backwards compatible schema"
    );
  }

  /**
   * getConfigInScope resolves every scope by merging the same tiers field by field, so a scope's
   * inherited values must not depend on whether it happens to carry a record of its own. Asserts
   * that invariant by reading the effective config before and after giving the scope a record
   * that touches none of the fields checked.
   */
  private void assertInheritedConfigUnaffectedByAnUnrelatedLocalField(String scope)
      throws Exception {
    Config before = restApp.restClient.getConfig(
        RestService.DEFAULT_REQUEST_PROPERTIES, scope, true);

    ConfigUpdateRequest groupOnly = new ConfigUpdateRequest();
    groupOnly.setCompatibilityGroup("application.version");
    restApp.restClient.updateConfig(groupOnly, scope);

    Config after = restApp.restClient.getConfig(
        RestService.DEFAULT_REQUEST_PROPERTIES, scope, true);

    assertEquals(before.getCompatibilityLevel(), after.getCompatibilityLevel(),
        "Inherited compatibilityLevel for " + scope + " changed after setting an unrelated "
            + "local field");
    assertEquals(before.getCompatibilityPolicy(), after.getCompatibilityPolicy(),
        "Inherited compatibilityPolicy for " + scope + " changed after setting an unrelated "
            + "local field");
    // Rule sets drive encryption and transforms at registration, so losing one here is a
    // data-plane change, not just a different answer from the config endpoint.
    assertEquals(before.getDefaultMetadata(), after.getDefaultMetadata(),
        "Inherited defaultMetadata for " + scope + " changed after setting an unrelated "
            + "local field");
    assertEquals(before.getOverrideMetadata(), after.getOverrideMetadata(),
        "Inherited overrideMetadata for " + scope + " changed after setting an unrelated "
            + "local field");
    assertEquals(before.getDefaultRuleSet(), after.getDefaultRuleSet(),
        "Inherited defaultRuleSet for " + scope + " changed after setting an unrelated "
            + "local field");
    assertEquals(before.getOverrideRuleSet(), after.getOverrideRuleSet(),
        "Inherited overrideRuleSet for " + scope + " changed after setting an unrelated "
            + "local field");
  }

  @Test
  public void testInheritedConfigIsIndependentOfUnrelatedLocalFields() throws Exception {
    // Set each tier to a distinct value so any tier that gets skipped or wrongly consulted
    // changes the resolved answer: the global context is the last-resort parent, the tenant-wide
    // config covers the default context, and a custom context covers its own subjects.
    // Metadata and rule sets ride along on the outermost tier so the assertions about them are
    // not vacuous: every scope below should still see them once it has a record of its own.
    ConfigUpdateRequest globalContext = new ConfigUpdateRequest();
    globalContext.setCompatibilityLevel(CompatibilityLevel.NONE.name);
    globalContext.setDefaultMetadata(
        new Metadata(null, Collections.singletonMap("owner", "platform"), null));
    globalContext.setOverrideRuleSet(new RuleSet(Collections.emptyList(),
        Collections.singletonList(new Rule("checkLen", null, RuleKind.CONDITION,
            RuleMode.WRITE, "CEL", null, null, "size(message.f1) < 100", null, null, false))));
    restApp.restClient.updateConfig(globalContext, ":.__GLOBAL:");

    ConfigUpdateRequest tenantWide = new ConfigUpdateRequest();
    tenantWide.setCompatibilityLevel(CompatibilityLevel.FULL.name);
    restApp.restClient.updateConfig(tenantWide, null);

    ConfigUpdateRequest customContext = new ConfigUpdateRequest();
    customContext.setCompatibilityLevel(CompatibilityLevel.BACKWARD_TRANSITIVE.name);
    restApp.restClient.updateConfig(customContext, ":.inherit:");

    assertInheritedConfigUnaffectedByAnUnrelatedLocalField("defaultContextSubject");
    assertInheritedConfigUnaffectedByAnUnrelatedLocalField(":.inherit:contextSubject");
    assertInheritedConfigUnaffectedByAnUnrelatedLocalField(":.noConfigOfItsOwn:ctxSubject");
    // A bare context scope: its parent is the global context, never the tenant-wide config.
    assertInheritedConfigUnaffectedByAnUnrelatedLocalField(":.bareContext:");
  }

  @Test
  public void testClearingABareContextOverrideResolvesTheGlobalContextNotTenantWide()
      throws Exception {
    // The tenant-wide config sets a policy, but a bare context does not inherit from it -- its
    // parent is the global context, which sets none here. Clearing the context's own override
    // must therefore fall through to the global context rather than to the tenant-wide config.
    String context = ":.bareParent:";

    ConfigUpdateRequest tenantWide = new ConfigUpdateRequest();
    tenantWide.setCompatibilityPolicy("LOGICAL");
    assertEquals(
        "LOGICAL",
        restApp.restClient.updateConfig(tenantWide, null).getCompatibilityPolicy(),
        "Setting the tenant-wide policy should succeed"
    );

    ConfigUpdateRequest contextOverride = new ConfigUpdateRequest();
    contextOverride.setCompatibilityPolicy("STRICT");
    assertEquals(
        "STRICT",
        restApp.restClient.updateConfig(contextOverride, context).getCompatibilityPolicy(),
        "Setting a context-level policy override should succeed"
    );

    ConfigUpdateRequest clearOverrideAndSetForward = new ConfigUpdateRequest();
    clearOverrideAndSetForward.setCompatibilityPolicy(Optional.empty());
    clearOverrideAndSetForward.setCompatibilityLevel(CompatibilityLevel.FORWARD.name);
    restApp.restClient.updateConfig(clearOverrideAndSetForward, context);

    Config resolved = restApp.restClient.getConfig(
        RestService.DEFAULT_REQUEST_PROPERTIES, context, true);
    assertEquals(CompatibilityLevel.FORWARD.name, resolved.getCompatibilityLevel(),
        "the context should now be FORWARD");
    assertNull(resolved.getCompatibilityPolicy(),
        "the cleared policy should fall through to the global context, which sets none");
  }

  @Test
  public void testFieldsInheritIndependentlyFromDifferentTiers() throws Exception {
    // The policy lives at one tier and the level at another, with nothing set locally. Under
    // field-by-field inheritance both must come through; taking the nearest record whole would
    // drop the policy entirely, and would do so only for subjects that have no local record.
    ConfigUpdateRequest ctx = new ConfigUpdateRequest();
    ctx.setCompatibilityLevel(CompatibilityLevel.FULL.name);
    restApp.restClient.updateConfig(ctx, ":.split:");

    ConfigUpdateRequest globalContext = new ConfigUpdateRequest();
    globalContext.setCompatibilityPolicy("LOGICAL");
    restApp.restClient.updateConfig(globalContext, ":.__GLOBAL:");

    Config noRecord = restApp.restClient.getConfig(
        RestService.DEFAULT_REQUEST_PROPERTIES, ":.split:noRecordOfItsOwn", true);
    assertEquals(CompatibilityLevel.FULL.name, noRecord.getCompatibilityLevel(),
        "level should come from the owning context");
    assertEquals("LOGICAL", noRecord.getCompatibilityPolicy(),
        "policy should still be inherited from the global context");

    // The same scope shape, but with a local record touching neither field, must agree.
    ConfigUpdateRequest groupOnly = new ConfigUpdateRequest();
    groupOnly.setCompatibilityGroup("application.version");
    restApp.restClient.updateConfig(groupOnly, ":.split:hasARecord");
    Config hasRecord = restApp.restClient.getConfig(
        RestService.DEFAULT_REQUEST_PROPERTIES, ":.split:hasARecord", true);
    assertEquals(noRecord.getCompatibilityLevel(), hasRecord.getCompatibilityLevel(),
        "a local record on an unrelated field must not change the inherited level");
    assertEquals(noRecord.getCompatibilityPolicy(), hasRecord.getCompatibilityPolicy(),
        "a local record on an unrelated field must not change the inherited policy");
  }

  @Test
  public void testLogicalPolicyRejectsForwardCompatibilityInTheSameRequest() throws Exception {
    // Iceberg, the only current LOGICAL target, supports only backward-compatible evolution, so
    // this pairing must be rejected up front rather than accepted and left to fail at
    // registration time.
    ConfigUpdateRequest config = new ConfigUpdateRequest();
    config.setCompatibilityLevel(CompatibilityLevel.FORWARD.name);
    config.setCompatibilityPolicy("LOGICAL");
    try {
      restApp.restClient.updateConfig(config, null);
      fail("Setting FORWARD compatibility with LOGICAL policy should fail");
    } catch (RestClientException e) {
      assertEquals(
          RestInvalidCompatibilityException.ERROR_CODE,
          e.getErrorCode(),
          "Should get an invalid compatibility level error"
      );
    }
  }

  /** Asserts that setting only the policy is rejected because the level is inherited. */
  private void assertLogicalPolicyRejectedAgainstInheritedLevel(
      String subject, String expectedLevel) throws Exception {
    ConfigUpdateRequest subjectConfig = new ConfigUpdateRequest();
    subjectConfig.setCompatibilityPolicy("LOGICAL");
    try {
      restApp.restClient.updateConfig(subjectConfig, subject);
      fail("Setting LOGICAL policy for " + subject + " under an inherited " + expectedLevel
          + " compatibility level should fail");
    } catch (RestClientException e) {
      assertEquals(
          RestInvalidCompatibilityException.ERROR_CODE,
          e.getErrorCode(),
          "Should get an invalid compatibility level error"
      );
      assertTrue(
          e.getMessage().contains("compatibilityPolicy=LOGICAL")
              && e.getMessage().contains("compatibilityLevel=" + expectedLevel),
          "Should be rejected against the inherited level " + expectedLevel + ": "
              + e.getMessage()
      );
    }
  }

  @Test
  public void testLogicalPolicyRejectsAnInheritedForwardCompatibility() throws Exception {
    // The request sets only compatibilityPolicy; the level is inherited. Each tier uses a
    // distinct forward level so the rejection can only be attributed to the tier under test.

    // Inherited from the owning context, while the tenant-wide config is still the safe default.
    String context = ":.mycontext:";
    ConfigUpdateRequest contextConfig = new ConfigUpdateRequest();
    contextConfig.setCompatibilityLevel(CompatibilityLevel.FORWARD_TRANSITIVE.name);
    assertEquals(
        CompatibilityLevel.FORWARD_TRANSITIVE.name,
        restApp.restClient.updateConfig(contextConfig, context).getCompatibilityLevel(),
        "Setting the context-level compatibility level should succeed"
    );
    assertLogicalPolicyRejectedAgainstInheritedLevel(
        context + "contextSubject", CompatibilityLevel.FORWARD_TRANSITIVE.name);

    // Inherited from the tenant-wide config, for a subject in the default context.
    ConfigUpdateRequest globalConfig = new ConfigUpdateRequest();
    globalConfig.setCompatibilityLevel(CompatibilityLevel.FORWARD.name);
    assertEquals(
        CompatibilityLevel.FORWARD.name,
        restApp.restClient.updateConfig(globalConfig, null).getCompatibilityLevel(),
        "Changing global compatibility level should succeed"
    );
    assertLogicalPolicyRejectedAgainstInheritedLevel(
        "testSubject", CompatibilityLevel.FORWARD.name);
  }

  @Test
  public void testClearingAnOverrideDefersToTheRegistrationTimeCheck() throws Exception {
    // A cleared field takes its value from the scope's parent, which the config-write guard
    // deliberately does not try to re-derive. Such an update is therefore accepted, and the
    // resulting pairing is caught at registration instead.
    String subject = "testSubject";
    String schemaString1 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"}]}").canonicalString();
    assertEquals(
        expectedSchemaId(1),
        restApp.restClient.registerSchema(schemaString1, subject),
        "Registering should succeed"
    );

    ConfigUpdateRequest globalConfig = new ConfigUpdateRequest();
    globalConfig.setCompatibilityPolicy("LOGICAL");
    restApp.restClient.updateConfig(globalConfig, null);

    ConfigUpdateRequest subjectOverride = new ConfigUpdateRequest();
    subjectOverride.setCompatibilityPolicy("STRICT");
    restApp.restClient.updateConfig(subjectOverride, subject);

    // Clearing the override exposes the inherited LOGICAL alongside the FORWARD being set here.
    // The write is accepted rather than rejected up front.
    ConfigUpdateRequest clearOverrideAndSetForward = new ConfigUpdateRequest();
    clearOverrideAndSetForward.setCompatibilityPolicy(Optional.empty());
    clearOverrideAndSetForward.setCompatibilityLevel(CompatibilityLevel.FORWARD.name);
    assertEquals(
        CompatibilityLevel.FORWARD.name,
        restApp.restClient.updateConfig(clearOverrideAndSetForward, subject)
            .getCompatibilityLevel(),
        "Clearing an override is not validated up front"
    );

    String schemaString2 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"},"
        + " {\"type\":\"string\",\"name\":\"f2\",\"default\":\"x\"}]}").canonicalString();
    try {
      restApp.restClient.registerSchema(schemaString2, subject);
      fail("Registering under the resulting LOGICAL+FORWARD config should fail");
    } catch (RestClientException e) {
      assertEquals(
          RestIncompatibleSchemaException.DEFAULT_ERROR_CODE,
          e.getStatus(),
          "Should get a conflict status"
      );
      assertTrue(
          e.getMessage().contains("compatibilityPolicy=LOGICAL")
              && e.getMessage().contains("compatibilityLevel=FORWARD"),
          "Should be rejected by the LOGICAL+FORWARD guard at registration: " + e.getMessage()
      );
    }
  }

  @Test
  public void testLogicalPolicyRegistrationBackstopCatchesALaterGlobalForwardChange()
      throws Exception {
    // The config-write guard has no way to see this coming: the subject sets only
    // compatibilityPolicy (global is still the default at that point, so it succeeds), and only
    // afterward does an unrelated global-level change make the pairing invalid. getConfigInScope
    // must still resolve the subject's effective level to the real global value rather than
    // silently defaulting it, so the registration-time backstop is what has to catch this.
    String subject = "testSubject";

    String schemaString1 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"}]}").canonicalString();
    assertEquals(
        expectedSchemaId(1),
        restApp.restClient.registerSchema(schemaString1, subject),
        "Registering should succeed"
    );

    ConfigUpdateRequest subjectConfig = new ConfigUpdateRequest();
    subjectConfig.setCompatibilityPolicy("LOGICAL");
    assertEquals(
        "LOGICAL",
        restApp.restClient.updateConfig(subjectConfig, subject).getCompatibilityPolicy(),
        "Setting a subject-level LOGICAL policy should succeed"
    );

    ConfigUpdateRequest globalConfig = new ConfigUpdateRequest();
    globalConfig.setCompatibilityLevel(CompatibilityLevel.FORWARD.name);
    assertEquals(
        CompatibilityLevel.FORWARD.name,
        restApp.restClient.updateConfig(globalConfig, null).getCompatibilityLevel(),
        "Changing global compatibility level should succeed"
    );

    // A defaulted field addition is compatible under BACKWARD, FORWARD and FULL alike (and is not
    // a logical REQUIRED_FIELD_ADDED, since it carries a default), so a rejection here can only
    // come from the LOGICAL+FORWARD config guard itself, not from an incidental incompatibility.
    // (Registering schemaString1 again verbatim would short-circuit on the identical-schema fast
    // path before the compatibility check ever runs, so it must be a genuinely different schema.)
    String schemaString2 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"},"
        + " {\"type\":\"string\",\"name\":\"f2\",\"default\":\"x\"}]}").canonicalString();
    try {
      restApp.restClient.registerSchema(schemaString2, subject);
      fail("Registering under an effective LOGICAL+FORWARD config should fail");
    } catch (RestClientException e) {
      assertEquals(
          RestIncompatibleSchemaException.DEFAULT_ERROR_CODE,
          e.getStatus(),
          "Should get a conflict status"
      );
      assertTrue(
          e.getMessage().contains("compatibilityPolicy=LOGICAL")
              && e.getMessage().contains("compatibilityLevel=FORWARD"),
          "Should be rejected by the LOGICAL+FORWARD config guard specifically: "
              + e.getMessage()
      );
    }
  }

  @Test
  public void testCompatibilityGroup() throws Exception {
    String subject = "testSubject";

    ParsedSchema schema1 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"}]}");

    ConfigUpdateRequest config = new ConfigUpdateRequest();
    config.setCompatibilityGroup("application.version");
    config.setValidateFields(false);
    // add compatibility group
    assertEquals(
        config,
        restApp.restClient.updateConfig(config, null),
        "Adding compatibility group should succeed"
    );

    Map<String, String> properties = new HashMap<>();
    properties.put("application.version", "1");
    Metadata metadata1 = new Metadata(null, properties, null);
    RegisterSchemaRequest request1 = new RegisterSchemaRequest(schema1);
    request1.setMetadata(metadata1);
    int expectedIdSchema1 = expectedSchemaId(1);
    assertEquals(
        expectedIdSchema1,
        restApp.restClient.registerSchema(request1, subject, false).getId(),
        "Registering should succeed"
    );
    // verify that default compatibility level is backward
    assertEquals(
        CompatibilityLevel.BACKWARD.name,
        restApp.restClient.getConfig(null).getCompatibilityLevel(),
        "Default compatibility level should be backward"
    );

    // register forward compatible schema, which should fail
    ParsedSchema schema2 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"},"
        + " {\"type\":\"string\",\"name\":\"f2\"}]}");
    RegisterSchemaRequest request2 = new RegisterSchemaRequest(schema2);
    try {
      restApp.restClient.registerSchema(request2, subject, false);
      fail("Registering a forward compatible schema should fail");
    } catch (RestClientException e) {
      // this is expected.
      assertEquals(
          RestIncompatibleSchemaException.DEFAULT_ERROR_CODE,
          e.getStatus(),
          "Should get a conflict status"
      );
    }

    // now try registering a forward compatible schema in a different compatibility group
    properties = new HashMap<>();
    properties.put("application.version", "2");
    Metadata metadata2 = new Metadata(null, properties, null);
    request2.setMetadata(metadata2);
    int expectedIdSchema2 = expectedSchemaId(2);
    assertEquals(
        expectedIdSchema2,
        restApp.restClient.registerSchema(request2, subject, false).getId(),
        "Registering should succeed"
    );
  }


  @Test
  public void testAddCompatibilityGroup() throws Exception {
    String subject = "testSubject";

    ParsedSchema schema1 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"}]}");

    Map<String, String> properties = new HashMap<>();
    Metadata metadata1 = new Metadata(null, properties, null);
    RegisterSchemaRequest request1 = new RegisterSchemaRequest(schema1);
    request1.setMetadata(metadata1);
    int expectedIdSchema1 = expectedSchemaId(1);
    assertEquals(
        expectedIdSchema1,
        restApp.restClient.registerSchema(request1, subject, false).getId(),
        "Registering should succeed"
    );
    // verify that default compatibility level is backward
    assertEquals(
        CompatibilityLevel.BACKWARD.name,
        restApp.restClient.getConfig(null).getCompatibilityLevel(),
        "Default compatibility level should be backward"
    );

    // register forward compatible schema, which should fail
    ParsedSchema schema2 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"},"
        + " {\"type\":\"string\",\"name\":\"f2\"}]}");
    RegisterSchemaRequest request2 = new RegisterSchemaRequest(schema2);
    try {
      restApp.restClient.registerSchema(request2, subject, false);
      fail("Registering a forward compatible schema should fail");
    } catch (RestClientException e) {
      // this is expected.
      assertEquals(
          RestIncompatibleSchemaException.DEFAULT_ERROR_CODE,
          e.getStatus(),
          "Should get a conflict status"
      );
    }

    // Add compatibility group after first schema already registered
    ConfigUpdateRequest config = new ConfigUpdateRequest();
    config.setCompatibilityGroup("application.version");
    config.setValidateFields(false);
    // add compatibility group
    assertEquals(
        config,
        restApp.restClient.updateConfig(config, null),
        "Adding compatibility group should succeed"
    );

    // now try registering a forward compatible schema in a different compatibility group
    properties = new HashMap<>();
    properties.put("application.version", "2");
    Metadata metadata2 = new Metadata(null, properties, null);
    request2.setMetadata(metadata2);
    int expectedIdSchema2 = expectedSchemaId(2);
    assertEquals(
        expectedIdSchema2,
        restApp.restClient.registerSchema(request2, subject, false).getId(),
        "Registering should succeed"
    );

    ParsedSchema schema3 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"},"
        + " {\"type\":[\"null\", \"int\"],\"name\":\"f2\",\"default\":null}]}");

    properties = new HashMap<>();
    Metadata metadata3 = new Metadata(null, properties, null);
    RegisterSchemaRequest request3 = new RegisterSchemaRequest(schema3);
    request3.setMetadata(metadata3);
    int expectedIdSchema3 = expectedSchemaId(3);
    assertEquals(
        expectedIdSchema3,
        restApp.restClient.registerSchema(request3, subject, false).getId(),
        "Registering should succeed"
    );
  }

  @Test
  public void testClearCompatibilityGroup() throws Exception {
    String subject = "testSubject";

    ConfigUpdateRequest config = new ConfigUpdateRequest();
    config.setCompatibilityGroup("application.version");
    config.setValidateFields(true);
    // add compatibility group
    assertEquals(
        config,
        restApp.restClient.updateConfig(config, null)
    );

    ConfigUpdateRequest expectedConfig = new ConfigUpdateRequest();
    expectedConfig.setValidateFields(true);

    ConfigUpdateRequest newConfig = new ConfigUpdateRequest();
    newConfig.setCompatibilityGroup(Optional.empty());
    // clear compatibility group
    assertEquals(
        expectedConfig,
        restApp.restClient.updateConfig(newConfig, null)
    );
  }

  @Test
  public void testConfigMetadata() throws Exception {
    String subject = "testSubject";

    ParsedSchema schema1 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"}]}");

    Map<String, String> properties = new HashMap<>();
    properties.put("configKey", "configValue");
    Metadata metadata = new Metadata(null, properties, null);
    ConfigUpdateRequest config = new ConfigUpdateRequest();
    config.setDefaultMetadata(metadata);
    config.setValidateFields(false);
    // add config metadata
    assertEquals(
        config,
        restApp.restClient.updateConfig(config, null),
        "Adding config with initial metadata should succeed"
    );

    properties = new HashMap<>();
    properties.put("subjectKey", "subjectValue");
    Metadata metadata1 = new Metadata(null, properties, null);
    RegisterSchemaRequest request1 = new RegisterSchemaRequest(schema1);
    request1.setMetadata(metadata1);
    int expectedIdSchema1 = expectedSchemaId(1);
    RegisterSchemaResponse response = restApp.restClient.registerSchema(request1, subject, false);
    assertEquals(
        expectedIdSchema1,
        response.getId(),
        "Registering should succeed"
    );
    Metadata metadata2 = response.getMetadata();
    assertEquals("configValue", metadata2.getProperties().get("configKey"));
    assertEquals("subjectValue", metadata2.getProperties().get("subjectKey"));

    assertEquals(
        response.getVersion(),
        restApp.restClient.lookUpSubjectVersion(
            new RegisterSchemaRequest(
                new Schema(subject, response)), subject, false, false).getVersion(),
        "Version should match"
    );

    // verify that default compatibility level is backward
    assertEquals(
        CompatibilityLevel.BACKWARD.name,
        restApp.restClient.getConfig(null).getCompatibilityLevel(),
        "Default compatibility level should be backward"
    );

    // change it to forward
    assertEquals(
        CompatibilityLevel.FORWARD.name,
        restApp.restClient
            .updateCompatibility(CompatibilityLevel.FORWARD.name, null)
            .getCompatibilityLevel(),
        "Changing compatibility level should succeed"
    );

    // verify that new compatibility level is forward
    assertEquals(
        CompatibilityLevel.FORWARD.name,
        restApp.restClient.getConfig(null).getCompatibilityLevel(),
        "New compatibility level should be forward"
    );

    // register forward compatible schema
    ParsedSchema schema2 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"},"
        + " {\"type\":\"string\",\"name\":\"f2\"}]}");
    RegisterSchemaRequest request2 = new RegisterSchemaRequest(schema2);
    int expectedIdSchema2 = expectedSchemaId(2);
    response = restApp.restClient.registerSchema(request2, subject, false);
    assertEquals(
        expectedIdSchema2,
        response.getId(),
        "Registering should succeed"
    );
    metadata2 = response.getMetadata();
    assertEquals("configValue", metadata2.getProperties().get("configKey"));
    assertEquals("subjectValue", metadata2.getProperties().get("subjectKey"));

    assertEquals(
        response.getVersion(),
        restApp.restClient.lookUpSubjectVersion(
            new RegisterSchemaRequest(
                new Schema(subject, response)), subject, false, false).getVersion(),
        "Version should match"
    );

    SchemaString schemaString = restApp.restClient.getId(expectedIdSchema2, subject);
    metadata2 = schemaString.getMetadata();
    assertEquals("configValue", metadata2.getProperties().get("configKey"));
    assertEquals("subjectValue", metadata2.getProperties().get("subjectKey"));

    // re-register
    response = restApp.restClient.registerSchema(request2, subject, false);
    assertEquals(
        expectedIdSchema2,
        response.getId(),
        "Registering should succeed"
    );
    metadata2 = response.getMetadata();
    assertEquals("configValue", metadata2.getProperties().get("configKey"));
    assertEquals("subjectValue", metadata2.getProperties().get("subjectKey"));

    // register forward compatible schema with specified metadata
    ParsedSchema schema3 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"},"
        + " {\"type\":\"string\",\"name\":\"f2\"},"
        + " {\"type\":\"string\",\"name\":\"f3\"}]}");
    properties = new HashMap<>();
    properties.put("newSubjectKey", "newSubjectValue");
    Metadata metadata3 = new Metadata(null, properties, null);
    RegisterSchemaRequest request3 = new RegisterSchemaRequest(schema3);
    request3.setMetadata(metadata3);
    int expectedIdSchema3 = expectedSchemaId(3);
    response = restApp.restClient.registerSchema(request3, subject, false);
    assertEquals(
        expectedIdSchema3,
        response.getId(),
        "Registering should succeed"
    );
    Metadata metadata4 = response.getMetadata();
    assertEquals("configValue", metadata4.getProperties().get("configKey"));
    assertNull(metadata4.getProperties().get("subjectKey"));
    assertEquals("newSubjectValue", metadata4.getProperties().get("newSubjectKey"));

    assertEquals(
        response.getVersion(),
        restApp.restClient.lookUpSubjectVersion(
            new RegisterSchemaRequest(
                new Schema(subject, response)), subject, false, false).getVersion(),
        "Version should match"
    );

    schemaString = restApp.restClient.getId(expectedIdSchema3, subject);
    metadata4 = schemaString.getMetadata();
    assertEquals("configValue", metadata4.getProperties().get("configKey"));
    assertNull(metadata4.getProperties().get("subjectKey"));
    assertEquals("newSubjectValue", metadata4.getProperties().get("newSubjectKey"));
  }

  @Test
  public void testConfigRuleSet() throws Exception {
    String subject = "testSubject";

    ParsedSchema schema1 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"}]}");

    Rule r1 = new Rule("foo", null, null, RuleMode.UPGRADE, "IGNORE", null, null, null, null, null, false);
    List<Rule> rules = Collections.singletonList(r1);
    RuleSet ruleSet = new RuleSet(rules, null);
    ConfigUpdateRequest config = new ConfigUpdateRequest();
    config.setDefaultRuleSet(ruleSet);
    config.setValidateFields(false);
    // add config ruleSet
    assertEquals(
        config,
        restApp.restClient.updateConfig(config, null),
        "Adding config with initial ruleSet should succeed"
    );

    Rule r2 = new Rule("bar", null, null, RuleMode.UPGRADE, "type1", null, null, null, null, null, false);
    rules = Collections.singletonList(r2);
    ruleSet = new RuleSet(rules, null);
    RegisterSchemaRequest request1 = new RegisterSchemaRequest(schema1);
    request1.setRuleSet(ruleSet);
    int expectedIdSchema1 = expectedSchemaId(1);
    RegisterSchemaResponse response = restApp.restClient.registerSchema(request1, subject, false);
    assertEquals(
        expectedIdSchema1,
        response.getId(),
        "Registering should succeed"
    );
    RuleSet ruleSet2 = response.getRuleSet();
    assertEquals("foo", ruleSet2.getMigrationRules().get(0).getName());
    assertEquals("bar", ruleSet2.getMigrationRules().get(1).getName());

    assertEquals(
        response.getVersion(),
        restApp.restClient.lookUpSubjectVersion(
            new RegisterSchemaRequest(
                new Schema(subject, response)), subject, false, false).getVersion(),
        "Version should match"
    );

    List<Schema> schemas = restApp.restClient.getSchemas(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, false, "type1", null, null);
    assertEquals(1, schemas.size());
    schemas = restApp.restClient.getSchemas(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, true, "type1", null, null);
    assertEquals(1, schemas.size());

    // verify that default compatibility level is backward
    assertEquals(
        CompatibilityLevel.BACKWARD.name,
        restApp.restClient.getConfig(null).getCompatibilityLevel(),
        "Default compatibility level should be backward"
    );

    // change it to forward
    assertEquals(
        CompatibilityLevel.FORWARD.name,
        restApp.restClient
            .updateCompatibility(CompatibilityLevel.FORWARD.name, null)
            .getCompatibilityLevel(),
        "Changing compatibility level should succeed"
    );

    // verify that new compatibility level is forward
    assertEquals(
        CompatibilityLevel.FORWARD.name,
        restApp.restClient.getConfig(null).getCompatibilityLevel(),
        "New compatibility level should be forward"
    );

    // register forward compatible schema
    ParsedSchema schema2 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"},"
        + " {\"type\":\"string\",\"name\":\"f2\"}]}");
    RegisterSchemaRequest request2 = new RegisterSchemaRequest(schema2);
    int expectedIdSchema2 = expectedSchemaId(2);
    response = restApp.restClient.registerSchema(request2, subject, false);
    assertEquals(
        expectedIdSchema2,
        response.getId(),
        "Registering should succeed"
    );
    ruleSet2 = response.getRuleSet();
    assertEquals("foo", ruleSet2.getMigrationRules().get(0).getName());
    assertEquals("bar", ruleSet2.getMigrationRules().get(1).getName());

    assertEquals(
        response.getVersion(),
        restApp.restClient.lookUpSubjectVersion(
            new RegisterSchemaRequest(
                new Schema(subject, response)), subject, false, false).getVersion(),
        "Version should match"
    );

    SchemaString schemaString = restApp.restClient.getId(expectedIdSchema2, subject);
    ruleSet2 = schemaString.getRuleSet();
    assertEquals("foo", ruleSet2.getMigrationRules().get(0).getName());
    assertEquals("bar", ruleSet2.getMigrationRules().get(1).getName());

    schemas = restApp.restClient.getSchemas(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, false, "type1", null, null);
    assertEquals(2, schemas.size());
    schemas = restApp.restClient.getSchemas(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, true, "type1", null, null);
    assertEquals(1, schemas.size());

    // re-register
    response = restApp.restClient.registerSchema(request2, subject, false);
    assertEquals(
        expectedIdSchema2,
        response.getId(),
        "Registering should succeed"
    );
    ruleSet2 = schemaString.getRuleSet();
    assertEquals("foo", ruleSet2.getMigrationRules().get(0).getName());
    assertEquals("bar", ruleSet2.getMigrationRules().get(1).getName());

    // register forward compatible schema with specified metadata
    ParsedSchema schema3 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"},"
        + " {\"type\":\"string\",\"name\":\"f2\"},"
        + " {\"type\":\"string\",\"name\":\"f3\"}]}");
    Rule r3 = new Rule("zap", null, null, RuleMode.UPGRADE, "type2", null, null, null, null, null, false);
    rules = Collections.singletonList(r3);
    ruleSet = new RuleSet(rules, null);
    RegisterSchemaRequest request3 = new RegisterSchemaRequest(schema3);
    request3.setRuleSet(ruleSet);
    int expectedIdSchema3 = expectedSchemaId(3);
    response = restApp.restClient.registerSchema(request3, subject, false);
    assertEquals(
        expectedIdSchema3,
        response.getId(),
        "Registering should succeed"
    );
    RuleSet ruleSet3 = response.getRuleSet();
    assertEquals("foo", ruleSet3.getMigrationRules().get(0).getName());
    assertEquals("zap", ruleSet3.getMigrationRules().get(1).getName());

    assertEquals(
        response.getVersion(),
        restApp.restClient.lookUpSubjectVersion(
            new RegisterSchemaRequest(
                new Schema(subject, response)), subject, false, false).getVersion(),
        "Version should match"
    );

    schemaString = restApp.restClient.getId(expectedIdSchema3, subject);
    ruleSet3 = schemaString.getRuleSet();
    assertEquals("foo", ruleSet3.getMigrationRules().get(0).getName());
    assertEquals("zap", ruleSet3.getMigrationRules().get(1).getName());

    schemas = restApp.restClient.getSchemas(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, false, "type1", null, null);
    assertEquals(2, schemas.size());
    schemas = restApp.restClient.getSchemas(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, false, "type2", null, null);
    assertEquals(1, schemas.size());
    schemas = restApp.restClient.getSchemas(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, true, "type1", null, null);
    assertEquals(0, schemas.size());
    schemas = restApp.restClient.getSchemas(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, true, "type2", null, null);
    assertEquals(1, schemas.size());
  }

  @Test
  public void testSchemaMetadata() throws Exception {
    String subject = "testSubject";

    ParsedSchema schema1 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"}]}");

    RegisterSchemaRequest request1 = new RegisterSchemaRequest(schema1);
    int expectedIdSchema1 = expectedSchemaId(1);
    assertEquals(
        expectedIdSchema1,
        restApp.restClient.registerSchema(request1, subject, false).getId(),
        "Registering should succeed"
    );

    // register just metadata, schema should be inherited from version 1
    RegisterSchemaRequest request2 = new RegisterSchemaRequest();
    Map<String, String> properties = new HashMap<>();
    properties.put("subjectKey", "subjectValue");
    Metadata metadata = new Metadata(null, properties, null);
    request2.setMetadata(metadata);
    int expectedIdSchema2 = expectedSchemaId(2);
    assertEquals(
        expectedIdSchema2,
        restApp.restClient.registerSchema(request2, subject, false).getId(),
        "Registering should succeed"
    );

    SchemaString schemaString = restApp.restClient.getId(expectedIdSchema2, subject);
    assertEquals(schema1.canonicalString(), schemaString.getSchemaString());
    assertEquals(metadata, schemaString.getMetadata());
  }

  @Test
  public void testSchemaRuleSet() throws Exception {
    String subject = "testSubject";

    ParsedSchema schema1 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"}]}");

    RegisterSchemaRequest request1 = new RegisterSchemaRequest(schema1);
    int expectedIdSchema1 = expectedSchemaId(1);
    assertEquals(
        expectedIdSchema1,
        restApp.restClient.registerSchema(request1, subject, false).getId(),
        "Registering should succeed"
    );

    // register just ruleSet, schema should be inherited from version 1
    RegisterSchemaRequest request2 = new RegisterSchemaRequest();
    Rule r1 = new Rule("foo", null, null, RuleMode.UPGRADE, "IGNORE", null, null, null, null, null, false);
    List<Rule> rules = Collections.singletonList(r1);
    RuleSet ruleSet = new RuleSet(rules, null);
    request2.setRuleSet(ruleSet);
    int expectedIdSchema2 = expectedSchemaId(2);
    assertEquals(
        expectedIdSchema2,
        restApp.restClient.registerSchema(request2, subject, false).getId(),
        "Registering should succeed"
    );

    SchemaString schemaString = restApp.restClient.getId(expectedIdSchema2, subject);
    assertEquals(schema1.canonicalString(), schemaString.getSchemaString());
    assertEquals(ruleSet, schemaString.getRuleSet());
  }

  @Test
  public void testCompareAndSetVersion() throws Exception {
    String subject = "testSubject";

    // register a valid avro
    String schemaString1 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"}]}").canonicalString();
    int expectedIdSchema1 = expectedSchemaId(1);
    assertEquals(
        expectedIdSchema1,
        restApp.restClient.registerSchema(schemaString1, subject),
        "Registering should succeed"
    );

    // register a backward compatible avro with wrong version number
    ParsedSchema schema2 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"},"
        + " {\"type\":\"string\",\"name\":\"f2\", \"default\": \"foo\"}]}");
    RegisterSchemaRequest request2 = new RegisterSchemaRequest(schema2);
    request2.setVersion(3);
    try {
      restApp.restClient.registerSchema(request2, subject, false);
      fail("Registering a wrong version should fail");
    } catch (RestClientException e) {
      // this is expected.
      assertEquals(
          RestInvalidSchemaException.ERROR_CODE,
          e.getErrorCode(),
          "Should get a bad request status"
      );
    }

    // register a backward compatible avro with right version number
    request2.setVersion(2);
    int expectedIdSchema2 = expectedSchemaId(2);
    assertEquals(
        expectedIdSchema2,
        restApp.restClient.registerSchema(request2, subject, false).getId(),
        "Registering should succeed"
    );
  }

  @Test
  public void testConfigInvalidRuleSet() throws Exception {
    Rule r1 = new Rule("foo", null, null, RuleMode.READ, "IGNORE", null, null, null, null, null, false);
    List<Rule> rules = Collections.singletonList(r1);
    // Add READ rule to migrationRules
    RuleSet ruleSet = new RuleSet(rules, null);
    ConfigUpdateRequest config = new ConfigUpdateRequest();
    config.setDefaultRuleSet(ruleSet);
    // add config ruleSet
    try {
      restApp.restClient.updateConfig(config, null);
      fail("Registering an invalid ruleSet should fail");
    } catch (RestClientException e) {
      // this is expected.
      assertEquals(
          RestInvalidRuleSetException.DEFAULT_ERROR_CODE,
          e.getStatus(),
          "Should get a bad request status"
      );
    }

    // Add rule with duplicate name
    Rule r2 = new Rule("foo", null, null, RuleMode.READ, "IGNORE", null, null, null, null, null, false);
    rules = ImmutableList.of(r1, r2);
    ruleSet = new RuleSet(null, rules);
    config = new ConfigUpdateRequest();
    config.setDefaultRuleSet(ruleSet);
    // add config ruleSet
    try {
      restApp.restClient.updateConfig(config, null);
      fail("Registering an invalid ruleSet should fail");
    } catch (RestClientException e) {
      // this is expected.
      assertEquals(
          RestInvalidRuleSetException.DEFAULT_ERROR_CODE,
          e.getStatus(),
          "Should get a bad request status"
      );
    }
  }

  @Test
  public void testRegisterInvalidRuleSet() throws Exception {
    String subject = "testSubject";

    ParsedSchema schema1 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"}]}");

    Rule r1 = new Rule("foo", null, null, RuleMode.READ, null, null, null, null, null, null, false);
    List<Rule> rules = Collections.singletonList(r1);
    RuleSet ruleSet = new RuleSet(rules, null);
    RegisterSchemaRequest request1 = new RegisterSchemaRequest(schema1);
    request1.setRuleSet(ruleSet);
    try {
      restApp.restClient.registerSchema(request1, subject, false);
      fail("Registering an invalid ruleSet should fail");
    } catch (RestClientException e) {
      // this is expected.
      assertEquals(
          RestInvalidRuleSetException.DEFAULT_ERROR_CODE,
          e.getStatus(),
          "Should get a bad request status"
      );
    }
  }

  @Test
  public void testRegisterBadDefaultWithValidateNewSchemaConfig() throws Exception {
    String subject = "testSubject";

    String schemaString = "{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"int\",\"default\":\"foo\",\"name\":"
        + "\"f" + "\"}]}";
    String schema = AvroUtils.parseSchema(schemaString).canonicalString();

    ConfigUpdateRequest config = new ConfigUpdateRequest();
    config.setValidateNewSchemas(false);
    assertEquals(
        config,
        restApp.restClient.updateConfig(config, null),
        "Setting normalize config should succeed"
    );

    List<String> errors = restApp.restClient.testCompatibility(schema, subject, "latest");
    assertTrue(errors.isEmpty());

    config = new ConfigUpdateRequest();
    config.setValidateNewSchemas(true);
    assertEquals(
        config,
        restApp.restClient.updateConfig(config, null),
        "Setting normalize config should succeed"
    );

    try {
      restApp.restClient.testCompatibility(schema, subject, "latest");
      fail("Testing compatibility for schema with invalid default should fail with "
          + Errors.INVALID_SCHEMA_ERROR_CODE
          + " (invalid schema)");
    } catch (RestClientException rce) {
      assertEquals(Errors.INVALID_SCHEMA_ERROR_CODE, rce.getErrorCode());
    }

    try {
      restApp.restClient.registerSchema(schema, subject);
      fail("Registering schema with invalid default should fail with "
          + Errors.INVALID_SCHEMA_ERROR_CODE
          + " (invalid schema)");
    } catch (RestClientException rce) {
      assertEquals(Errors.INVALID_SCHEMA_ERROR_CODE, rce.getErrorCode());
    }
  }

  @Test
  public void testGlobalNormalizeInheritedWhenSubjectConfigSet() throws Exception {
    String subject = "testSubject";

    // Set global normalize = true
    ConfigUpdateRequest globalConfig = new ConfigUpdateRequest();
    globalConfig.setNormalize(true);
    assertEquals(globalConfig,
        restApp.restClient.updateConfig(globalConfig, null),
        "Setting global normalize config should succeed");

    // Set a subject-level config that does NOT explicitly set normalize.
    ConfigUpdateRequest subjectConfig = new ConfigUpdateRequest();
    subjectConfig.setCompatibilityLevel(CompatibilityLevel.BACKWARD.name);
    assertEquals(subjectConfig,
        restApp.restClient.updateConfig(subjectConfig, subject),
        "Setting subject-level compatibility should succeed");

    // A schema whose int field has a string default is only rejected when
    // normalization (and the strict validation it triggers) is active. If
    // normalize is inherited correctly from the global config, registration
    // and compatibility check should fail with INVALID_SCHEMA_ERROR_CODE.
    String schemaString = "{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"int\",\"default\":\"foo\",\"name\":"
        + "\"f" + "\"}]}";
    String schema = AvroUtils.parseSchema(schemaString).canonicalString();

    try {
      restApp.restClient.testCompatibility(schema, subject, "latest");
      fail("Testing compatibility for schema with invalid default should fail with "
          + Errors.INVALID_SCHEMA_ERROR_CODE
          + " when normalize is inherited from the global config");
    } catch (RestClientException rce) {
      assertEquals(Errors.INVALID_SCHEMA_ERROR_CODE, rce.getErrorCode(), "Invalid schema");
    }

    try {
      restApp.restClient.registerSchema(schema, subject);
      fail("Registering schema with invalid default should fail with "
          + Errors.INVALID_SCHEMA_ERROR_CODE
          + " when normalize is inherited from the global config");
    } catch (RestClientException rce) {
      assertEquals(Errors.INVALID_SCHEMA_ERROR_CODE, rce.getErrorCode(), "Invalid schema");
    }
  }

  @Test
  public void testSubjectAlias() throws Exception {
    String subject = "testSubject";

    // register a valid avro
    String schemaString1 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"}]}").canonicalString();
    int expectedIdSchema1 = expectedSchemaId(1);
    assertEquals(
        expectedIdSchema1,
        restApp.restClient.registerSchema(schemaString1, subject),
        "Registering should succeed"
    );

    ConfigUpdateRequest config = new ConfigUpdateRequest();
    config.setAlias("testSubject");
    // set alias config
    assertEquals(
        config,
        restApp.restClient.updateConfig(config, "testAlias"),
        "Setting alias config should succeed"
    );

    Schema schema = restApp.restClient.getVersion("testAlias", 1);
    assertEquals(schemaString1, schema.getSchema());
  }

  @Test
  public void testSubjectAliasWithSlash() throws Exception {
    String subject = "testSubject";

    // register a valid avro
    String schemaString1 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"}]}").canonicalString();
    int expectedIdSchema1 = expectedSchemaId(1);
    assertEquals(
        expectedIdSchema1,
        restApp.restClient.registerSchema(schemaString1, subject),
        "Registering should succeed"
    );

    ConfigUpdateRequest config = new ConfigUpdateRequest();
    config.setAlias("testSubject");
    // set alias config
    assertEquals(
        config,
        restApp.restClient.updateConfig(config, "test/Alias"),
        "Setting alias config should succeed"
    );

    Schema schema = restApp.restClient.getVersion("test/Alias", 1);
    assertEquals(schemaString1, schema.getSchema());
  }

  @Test
  public void testSubjectAliasWithContext() throws Exception {
    RestService restClient1 = new RestService(restApp.restConnect + "/contexts/.mycontext");
    RestService restClient2 = new RestService(restApp.restConnect + "/contexts/.mycontext2");
    testSubjectAliasWithContextImpl(restClient1, restClient2);
  }

  public void testSubjectAliasWithContextImpl(
      RestService restClient1,
      RestService restClient2) throws Exception {
    // register a valid avro
    String schemaString1 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"}]}").canonicalString();
    int expectedIdSchema1 = expectedSchemaId(1);
    assertEquals(
        expectedIdSchema1,
        restClient1.registerSchema(schemaString1, "testSubject"),
        "Registering should succeed"
    );

    ConfigUpdateRequest config = new ConfigUpdateRequest();
    config.setAlias(":.mycontext:testSubject");
    // set alias config
    assertEquals(
        config,
        restApp.restClient.updateConfig(config, ":.mycontext2:testAlias"),
        "Setting alias config should succeed"
    );

    Schema schema = restClient2.getVersion("testAlias", 1);
    assertEquals(schemaString1, schema.getSchema());
  }

  @Test
  public void testGlobalAliasNotUsed() throws Exception {
    String subject = "testSubject";

    // register a valid avro
    String schemaString1 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"}]}").canonicalString();
    int expectedIdSchema1 = expectedSchemaId(1);
    assertEquals(
        expectedIdSchema1,
        restApp.restClient.registerSchema(schemaString1, subject),
        "Registering should succeed"
    );

    ConfigUpdateRequest config = new ConfigUpdateRequest();
    config.setAlias("badSubject");
    config.setValidateFields(false);
    // set global alias config
    assertEquals(
        config,
        restApp.restClient.updateConfig(config, null),
        "Setting alias config should succeed"
    );

    Schema schema = restApp.restClient.getVersion("testSubject", 1);
    assertEquals(schemaString1, schema.getSchema());
  }

  @Test
  public void testGetSchemasWithAliases() throws Exception {
    String subject = "testSubject";

    // register a valid avro
    String schemaString1 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"}]}").canonicalString();
    int expectedIdSchema1 = expectedSchemaId(1);
    assertEquals(
        expectedIdSchema1,
        restApp.restClient.registerSchema(schemaString1, subject),
        "Registering should succeed"
    );

    // register a backward compatible avro
    String schemaString2 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"},"
        + " {\"type\":\"string\",\"name\":\"f2\", \"default\": \"foo\"}]}").canonicalString();
    int expectedIdSchema2 = expectedSchemaId(2);
    assertEquals(
        expectedIdSchema2,
        restApp.restClient.registerSchema(schemaString2, subject),
        "Registering a compatible schema should succeed"
    );

    subject = "noTestSubject";

    // register unrelated schemas
    String unrelated1 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"x1\"}]}").canonicalString();
    int expectedIdUnrelated1 = expectedSchemaId(3);
    assertEquals(
        expectedIdUnrelated1,
        restApp.restClient.registerSchema(unrelated1, subject),
        "Registering should succeed"
    );

    // register a backward compatible avro
    String unrelated2 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"x1\"},"
        + " {\"type\":\"string\",\"name\":\"x2\", \"default\": \"foo\"}]}").canonicalString();
    int expectedIdUnrelated2 = expectedSchemaId(4);
    assertEquals(
        expectedIdUnrelated2,
        restApp.restClient.registerSchema(unrelated2, subject),
        "Registering a compatible schema should succeed"
    );

    ConfigUpdateRequest config = new ConfigUpdateRequest();
    config.setAlias("testSubject");
    // set alias config
    assertEquals(
        config,
        restApp.restClient.updateConfig(config, "testAlias"),
        "Setting alias config should succeed"
    );

    List<Schema> schemas = restApp.restClient.getSchemas("testAlias", true, false);
    assertEquals(0, schemas.size());

    List<ExtendedSchema> schemasWithAliases = restApp.restClient.getSchemas(
        RestService.DEFAULT_REQUEST_PROPERTIES, "testAlias", true, false, false, null, null, null);
    assertEquals(2, schemasWithAliases.size());
    for (ExtendedSchema schema : schemasWithAliases) {
      if (schema.getSubject().equals("testSubject")) {
        assertEquals(1, schema.getAliases().size());
        assertEquals("testAlias", schema.getAliases().get(0));
      } else {
        fail("Unexpected subject: " + schema.getSubject());
      }
    }

    subject = "testAlligator";
    String schemaString3 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"a1\"},"
        + " {\"type\":\"string\",\"name\":\"a2\", \"default\": \"foo\"}]}").canonicalString();
    int expectedIdSchema3 = expectedSchemaId(5);
    assertEquals(
        expectedIdSchema3,
        restApp.restClient.registerSchema(schemaString3, subject),
        "Registering a schema should succeed"
    );

    // see if the query picks up the new schema
    schemasWithAliases = restApp.restClient.getSchemas(
        RestService.DEFAULT_REQUEST_PROPERTIES, "testAl", true, false, false, null, null, null);
    assertEquals(3, schemasWithAliases.size());
    for (ExtendedSchema schema : schemasWithAliases) {
      if (schema.getSubject().endsWith("testAlligator")) {
        assertNull(schema.getAliases());
      } else if (schema.getSubject().equals("testSubject")) {
        assertEquals(1, schema.getAliases().size());
        assertEquals("testAlias", schema.getAliases().get(0));
      } else {
        fail("Unexpected subject: " + schema.getSubject());
      }
    }

    // make sure we don't get repeats with a common subjectPrefix
    schemasWithAliases = restApp.restClient.getSchemas(
        RestService.DEFAULT_REQUEST_PROPERTIES, "test", true, false, false, null, null, null);
    assertEquals(3, schemasWithAliases.size());
    for (ExtendedSchema schema : schemasWithAliases) {
      if (schema.getSubject().endsWith("testAlligator")) {
        assertNull(schema.getAliases());
      } else if (schema.getSubject().equals("testSubject")) {
        assertEquals(1, schema.getAliases().size());
        assertEquals("testAlias", schema.getAliases().get(0));
      } else {
        fail("Unexpected subject: " + schema.getSubject());
      }
    }

    // another alias to same subject
    config = new ConfigUpdateRequest();
    config.setAlias("testSubject");
    // set alias config
    assertEquals(
        config,
        restApp.restClient.updateConfig(config, "testAlias2"),
        "Setting alias config should succeed"
    );

    // see if the query picks up the new schema
    schemasWithAliases = restApp.restClient.getSchemas(
        RestService.DEFAULT_REQUEST_PROPERTIES, "testAl", true, false, false, null, null, null);
    assertEquals(3, schemasWithAliases.size());
    for (ExtendedSchema schema : schemasWithAliases) {
      if (schema.getSubject().endsWith("testAlligator")) {
        assertNull(schema.getAliases());
      } else if (schema.getSubject().equals("testSubject")) {
        assertEquals(2, schema.getAliases().size());
        assertEquals("testAlias", schema.getAliases().get(0));
        assertEquals("testAlias2", schema.getAliases().get(1));
      } else {
        fail("Unexpected subject: " + schema.getSubject());
      }
    }

    // make sure we don't get repeats with a common subjectPrefix
    schemasWithAliases = restApp.restClient.getSchemas(
        RestService.DEFAULT_REQUEST_PROPERTIES, "test", true, false, false, null, null, null);
    assertEquals(3, schemasWithAliases.size());
    for (ExtendedSchema schema : schemasWithAliases) {
      if (schema.getSubject().endsWith("testAlligator")) {
        assertNull(schema.getAliases());
      } else if (schema.getSubject().equals("testSubject")) {
        assertEquals(2, schema.getAliases().size());
        assertEquals("testAlias", schema.getAliases().get(0));
        assertEquals("testAlias2", schema.getAliases().get(1));
      } else {
        fail("Unexpected subject: " + schema.getSubject());
      }
    }
  }

  @Test
  public void testGetSchemasWithAliasesAndContextWildcard() throws Exception {
    String subject = "testSubject";

    // register a valid avro
    String schemaString1 = AvroUtils.parseSchema("{\"type\":\"record\","
            + "\"name\":\"myrecord\","
            + "\"fields\":"
            + "[{\"type\":\"string\",\"name\":\"f1\"}]}").canonicalString();
    int expectedIdSchema1 = expectedSchemaId(1);
    assertEquals(
        expectedIdSchema1,
        restApp.restClient.registerSchema(schemaString1, subject),
        "Registering should succeed"
    );

    // register a backward compatible avro
    String schemaString2 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"},"
        + " {\"type\":\"string\",\"name\":\"f2\", \"default\": \"foo\"}]}").canonicalString();
    int expectedIdSchema2 = expectedSchemaId(2);
    assertEquals(
        expectedIdSchema2,
        restApp.restClient.registerSchema(schemaString2, subject),
        "Registering a compatible schema should succeed"
    );

    subject = "noTestSubject";

    // register unrelated schemas
    String unrelated1 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"x1\"}]}").canonicalString();
    int expectedIdUnrelated1 = expectedSchemaId(3);
    assertEquals(
        expectedIdUnrelated1,
        restApp.restClient.registerSchema(unrelated1, subject),
        "Registering should succeed"
    );

    // register a backward compatible avro
    String unrelated2 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"x1\"},"
        + " {\"type\":\"string\",\"name\":\"x2\", \"default\": \"foo\"}]}").canonicalString();
    int expectedIdUnrelated2 = expectedSchemaId(4);
    assertEquals(
        expectedIdUnrelated2,
        restApp.restClient.registerSchema(unrelated2, subject),
        "Registering a compatible schema should succeed"
    );

    ConfigUpdateRequest config = new ConfigUpdateRequest();
    config.setAlias("testSubject");
    // set alias config
    assertEquals(
        config,
        restApp.restClient.updateConfig(config, "testAlias"),
        "Setting alias config should succeed"
    );

    List<Schema> schemas = restApp.restClient.getSchemas("testAlias", true, false);
    assertEquals(0, schemas.size());

    List<ExtendedSchema> schemasWithAliases = restApp.restClient.getSchemas(
        RestService.DEFAULT_REQUEST_PROPERTIES, ":*:", true, false, false, null, null, null);
    assertEquals(4, schemasWithAliases.size());
    for (ExtendedSchema schema : schemasWithAliases) {
      if (schema.getSubject().equals("testSubject")) {
        assertEquals(1, schema.getAliases().size());
        assertEquals("testAlias", schema.getAliases().get(0));
      } else {
        assertNull(schema.getAliases());
      }
    }

    subject = "testAlligator";
    String schemaString3 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"a1\"},"
        + " {\"type\":\"string\",\"name\":\"a2\", \"default\": \"foo\"}]}").canonicalString();
    int expectedIdSchema3 = expectedSchemaId(5);
    assertEquals(
        expectedIdSchema3,
        restApp.restClient.registerSchema(schemaString3, subject),
        "Registering a schema should succeed"
    );

    // see if the query picks up the new schema
    schemasWithAliases = restApp.restClient.getSchemas(
        RestService.DEFAULT_REQUEST_PROPERTIES, ":*:", true, false, false, null, null, null);
    assertEquals(5, schemasWithAliases.size());
    for (ExtendedSchema schema : schemasWithAliases) {
      if (schema.getSubject().equals("testSubject")) {
        assertEquals(1, schema.getAliases().size());
        assertEquals("testAlias", schema.getAliases().get(0));
      } else {
        assertNull(schema.getAliases());
      }
    }

    // another alias to same subject
    config = new ConfigUpdateRequest();
    config.setAlias("testSubject");
    // set alias config
    assertEquals(
        config,
        restApp.restClient.updateConfig(config, "testAlias2"),
        "Setting alias config should succeed"
    );

    // see if the query picks up the new schema
    schemasWithAliases = restApp.restClient.getSchemas(
        RestService.DEFAULT_REQUEST_PROPERTIES, ":*:", true, false, false, null, null, null);
    assertEquals(5, schemasWithAliases.size());
    for (ExtendedSchema schema : schemasWithAliases) {
      if (schema.getSubject().equals("testSubject")) {
        assertEquals(2, schema.getAliases().size());
        assertEquals("testAlias", schema.getAliases().get(0));
        assertEquals("testAlias2", schema.getAliases().get(1));
      } else {
        assertNull(schema.getAliases());
      }
    }

    subject = ":.myctx:testSubject";
    String schemaString4 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"b1\"},"
        + " {\"type\":\"string\",\"name\":\"b2\", \"default\": \"foo\"}]}").canonicalString();
    int expectedIdSchema4 = expectedSchemaId(1);
    assertEquals(
        expectedIdSchema4,
        restApp.restClient.registerSchema(schemaString4, subject),
        "Registering a schema should succeed"
    );

    // another alias to same subject
    config = new ConfigUpdateRequest();
    config.setAlias("testSubject");
    // set alias config
    assertEquals(
        config,
        restApp.restClient.updateConfig(config, ":.myctx:testAlias3"),
        "Setting alias config should succeed"
    );

    // see if the query picks up the new schema
    schemasWithAliases = restApp.restClient.getSchemas(
        RestService.DEFAULT_REQUEST_PROPERTIES, ":*:", true, false, false, null, null, null);
    assertEquals(6, schemasWithAliases.size());
    for (ExtendedSchema schema : schemasWithAliases) {
      if (schema.getSubject().equals("testSubject")) {
        assertEquals(2, schema.getAliases().size());
        assertEquals("testAlias", schema.getAliases().get(0));
        assertEquals("testAlias2", schema.getAliases().get(1));
      } else if (schema.getSubject().equals(":.myctx:testSubject")) {
        assertEquals(1, schema.getAliases().size());
        assertEquals(":.myctx:testAlias3", schema.getAliases().get(0));
      } else {
        assertNull(schema.getAliases());
      }
    }
  }

  @Test
  public void testRegisterEmptyRuleSet() throws Exception {
    String subject = "testSubject";

    ParsedSchema schema1 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"}]}");

    List<Rule> rules = Collections.emptyList();
    RuleSet ruleSet = new RuleSet(null, rules);
    RegisterSchemaRequest request1 = new RegisterSchemaRequest(schema1);
    request1.setRuleSet(ruleSet);
    int expectedIdSchema1 = expectedSchemaId(1);
    assertEquals(
        expectedIdSchema1,
        restApp.restClient.registerSchema(request1, subject, false).getId());

    request1.setRuleSet(null);
    Schema s = restApp.restClient.lookUpSubjectVersion(request1, subject, false, false);
    assertEquals(expectedIdSchema1, s.getId().intValue());

    Rule r1 = new Rule("foo", null, null, RuleMode.READ, "IGNORE", null, null, null, null, null, false);
    rules = ImmutableList.of(r1);
    ruleSet = new RuleSet(null, rules);
    RegisterSchemaRequest request2 = new RegisterSchemaRequest();
    request2.setRuleSet(ruleSet);

    // Register a rule set w/o a schema
    int expectedIdSchema2 = expectedSchemaId(2);
    assertEquals(
        expectedIdSchema2,
        restApp.restClient.registerSchema(request2, subject, false).getId());

    // Lookup the schema w/o the rule set
    s = restApp.restClient.lookUpSubjectVersion(request1, subject, false, false);
    assertEquals(expectedIdSchema2, s.getId().intValue());
  }

  @Test
  public void testGlobalContextWithNone() throws Exception {
    String subject = "testSubject";

    // register a valid avro
    String schemaString1 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"}]}").canonicalString();
    int expectedIdSchema1 = expectedSchemaId(1);
    assertEquals(
        expectedIdSchema1,
        restApp.restClient.registerSchema(schemaString1, subject),
        "Registering should succeed"
    );

    // register an incompatible avro
    String incompatibleSchemaString = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"},"
        + " {\"type\":\"string\",\"name\":\"f2\"}]}").canonicalString();
    try {
      restApp.restClient.registerSchema(incompatibleSchemaString, subject);
      fail("Registering an incompatible schema should fail");
    } catch (RestClientException e) {
      // this is expected.
      assertEquals(
          RestIncompatibleSchemaException.DEFAULT_ERROR_CODE,
          e.getStatus(),
          "Should get a conflict status"
      );
    }

    // change compatibility level to none in the global context and try again
    assertEquals(
        CompatibilityLevel.NONE.name,
        restApp.restClient
            .updateCompatibility(CompatibilityLevel.NONE.name, ":.__GLOBAL:")
            .getCompatibilityLevel(),
        "Changing compatibility level should succeed"
    );

    Config config = restApp.restClient.getConfig(RestService.DEFAULT_REQUEST_PROPERTIES, null, true);
    assertEquals("none", config.getCompatibilityLevel().toLowerCase());

    try {
      restApp.restClient.registerSchema(incompatibleSchemaString, subject);
    } catch (RestClientException e) {
      fail("Registering an incompatible schema should succeed after bumping down the compatibility "
          + "level to none");
    }
  }

  @Test
  public void testGetSchemasDeletedProperty() throws Exception {
    String subject = "testSubject";

    String schemaString = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"}]}").canonicalString();
    restApp.restClient.registerSchema(schemaString, subject);

    // Soft-delete the schema version
    restApp.restClient.deleteSchemaVersion(
        RestService.DEFAULT_REQUEST_PROPERTIES, subject, "1");

    // getSchemas with lookupDeletedSchema=true should include the deleted schema
    List<ExtendedSchema> schemas = restApp.restClient.getSchemas(
        RestService.DEFAULT_REQUEST_PROPERTIES, subject, false, true, false, null, null, null);
    assertEquals(1, schemas.size());
    assertTrue(schemas.get(0).getDeleted());
    assertNotNull(schemas.get(0).getTimestamp());

    // getSchemas with lookupDeletedSchema=false should not include the deleted schema
    schemas = restApp.restClient.getSchemas(
        RestService.DEFAULT_REQUEST_PROPERTIES, subject, false, false, false, null, null, null);
    assertEquals(0, schemas.size());
  }
}
