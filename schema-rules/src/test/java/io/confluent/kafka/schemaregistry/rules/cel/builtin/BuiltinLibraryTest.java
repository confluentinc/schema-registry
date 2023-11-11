/*
 * Copyright 2023 Confluent Inc.
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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.UUID;
import java.util.function.Predicate;
import org.junit.Test;

public class BuiltinLibraryTest {

    private static final String THERE_IS_NO_PLACE_LIKE = "127.0.0.1";

    private static final String IPV6_ADDR = "2001:db8:85a3:0:0:8a2e:370:7334";

    @Test
    public void emailFailure() {
        assertFailure("a.@b.com", BuiltinOverload::validateEmail);
    }

    @Test
    public void emailSuccess() {
        assertSuccess("a@b.com", BuiltinOverload::validateEmail);
    }

    @Test
    public void hostnameLengthFailure() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 256; ++i) {
            sb.append('a');
        }
        String subject = sb.toString();
        assertFailure(subject, BuiltinOverload::validateHostname);
    }

    @Test
    public void hostnameSuccess() {
        assertSuccess("localhost", BuiltinOverload::validateHostname);
    }

    @Test
    public void hostnameWithUnderscoresFailure() {
        assertFailure("not_a_valid_host_name", BuiltinOverload::validateHostname);
    }

    @Test
    public void ipv4Failure() {
        assertFailure("asd", BuiltinOverload::validateIpv4);
    }

    @Test
    public void ipv4LengthFailure() {
        assertFailure(IPV6_ADDR, BuiltinOverload::validateIpv4);
    }

    @Test
    public void ipv4Success() {
        assertSuccess(THERE_IS_NO_PLACE_LIKE, BuiltinOverload::validateIpv4);
    }

    @Test
    public void ipv6Failure() {
        assertFailure("asd", BuiltinOverload::validateIpv6);
    }

    @Test
    public void ipv6LengthFailure() {
        assertFailure(THERE_IS_NO_PLACE_LIKE, BuiltinOverload::validateIpv6);
    }

    @Test
    public void ipv6Success() {
        assertSuccess(IPV6_ADDR, BuiltinOverload::validateIpv6);
    }

    @Test
    public void uriFailure() {
        assertFailure("12 34", BuiltinOverload::validateUri);
    }

    @Test
    public void relativeUriFails() {
        assertFailure("//example.com", BuiltinOverload::validateUri);
    }

    @Test
    public void relativeUriRefFails() {
        assertFailure("abc", BuiltinOverload::validateUri);
    }

    @Test
    public void uriSuccess() {
        assertSuccess("http://example.org:8080/example.html", BuiltinOverload::validateUri);
    }

    @Test
    public void uriRefSuccess() {
        assertSuccess("http://foo.bar/?baz=qux#quux", BuiltinOverload::validateUriRef);
    }

    @Test
    public void relativeUriRefSuccess() {
        assertSuccess("//foo.bar/?baz=qux#quux", BuiltinOverload::validateUriRef);
    }

    @Test
    public void pathSuccess() {
        assertSuccess("/abc", BuiltinOverload::validateUriRef);
    }

    @Test
    public void illegalCharFailure() {
        assertFailure("\\\\WINDOWS\\fileshare", BuiltinOverload::validateUriRef);
    }

    @Test
    public void uuidFailure() {
        assertFailure("97cd-6e3d1bc14494", BuiltinOverload::validateUuid);
    }

    @Test
    public void uuidSuccess() {
        System.out.println(UUID.randomUUID());
        assertSuccess("fa02a430-892f-4160-97cd-6e3d1bc14494", BuiltinOverload::validateUuid);
    }
    static void assertSuccess(String input, Predicate<String> format) {
        assertTrue(format.test(input));
    }

    static void assertFailure(String input, Predicate<String> format) {
        assertFalse(format.test(input));
    }
}
