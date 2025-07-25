/*
 * Copyright 2024 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.client.rest;

import static org.junit.Assert.assertEquals;
import static org.mockito.AdditionalAnswers.returnsFirstArg;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.Mock;
import org.mockito.stubbing.Answer;

@RunWith(Parameterized.class)
public class RetryExecutorBackoffTest {

    @Parameters
    public static Collection<TestCase> parameters() throws Exception {
        return Arrays.asList(
            new TestCase().retriesAttempted(0)
                          .expectedMaxDelay(Duration.ofMillis(1000))
                          .expectedMedDelay(Duration.ofMillis(500))
                          .expectedMinDelay(Duration.ofMillis(1)),
            new TestCase().retriesAttempted(1)
                          .expectedMaxDelay(Duration.ofMillis(2000))
                          .expectedMedDelay(Duration.ofMillis(1000))
                          .expectedMinDelay(Duration.ofMillis(1)),
            new TestCase().retriesAttempted(2)
                          .expectedMaxDelay(Duration.ofMillis(4000))
                          .expectedMedDelay(Duration.ofMillis(2000))
                          .expectedMinDelay(Duration.ofMillis(1)),
            new TestCase().retriesAttempted(3)
                          .expectedMaxDelay(Duration.ofMillis(8000))
                          .expectedMedDelay(Duration.ofMillis(4000))
                          .expectedMinDelay(Duration.ofMillis(1)),
            new TestCase().retriesAttempted(4)
                          .expectedMaxDelay(Duration.ofMillis(16000))
                          .expectedMedDelay(Duration.ofMillis(8000))
                          .expectedMinDelay(Duration.ofMillis(1)),
            new TestCase().retriesAttempted(100)
                          .expectedMaxDelay(Duration.ofSeconds(20))
                          .expectedMedDelay(Duration.ofSeconds(10))
                          .expectedMinDelay(Duration.ofMillis(1))
        );
    }

    @Parameter
    public TestCase testCase;

    @Mock
    private Random mockRandom = mock(Random.class, withSettings().withoutAnnotations());

    @Before
    public void setUp() throws Exception {
        testCase.retryExecutor = new RetryExecutor(3, 1000, 20000, mockRandom);
    }

    @Test
    public void testMaxDelay() {
        mockMaxRandom();
        test(testCase.retryExecutor, testCase.retriesAttempted, testCase.expectedMaxDelay);
    }

    @Test
    public void testMedDelay() {
        mockMediumRandom();
        test(testCase.retryExecutor, testCase.retriesAttempted, testCase.expectedMedDelay);
    }

    @Test
    public void testMinDelay() {
        mockMinRandom();
        test(testCase.retryExecutor, testCase.retriesAttempted, testCase.expectedMinDelay);
    }

    private static void test(RetryExecutor retryExecutor, int retriesAttempted, Duration expectedDelay) {
        Duration computedDelay = retryExecutor.computeDelayBeforeNextRetry(retriesAttempted);
        assertEquals(expectedDelay, computedDelay);
    }

    private void mockMaxRandom() {
        when(mockRandom.nextInt(anyInt())).then((Answer<Integer>) invocationOnMock -> {
            Integer firstArg = (Integer) returnsFirstArg().answer(invocationOnMock);
            return firstArg - 1;
        });
    }

    private void mockMinRandom() {
        when(mockRandom.nextInt(anyInt())).then((Answer<Integer>) invocationOnMock -> {
            return 0;
        });
    }

    private void mockMediumRandom() {
        when(mockRandom.nextInt(anyInt())).then((Answer<Integer>) invocationOnMock -> {
            Integer firstArg = (Integer) returnsFirstArg().answer(invocationOnMock);
            return firstArg / 2 - 1;
        });
    }

    private static class TestCase {
        private RetryExecutor retryExecutor;
        private int retriesAttempted;
        private Duration expectedMinDelay;
        private Duration expectedMedDelay;
        private Duration expectedMaxDelay;

        public TestCase retryExecutor(RetryExecutor retryExecutor) {
            this.retryExecutor = retryExecutor;
            return this;
        }

        public TestCase retriesAttempted(int retriesAttempted) {
            this.retriesAttempted = retriesAttempted;
            return this;
        }

        public TestCase expectedMinDelay(Duration expectedMinDelay) {
            this.expectedMinDelay = expectedMinDelay;
            return this;
        }

        public TestCase expectedMedDelay(Duration expectedMedDelay) {
            this.expectedMedDelay = expectedMedDelay;
            return this;
        }

        public TestCase expectedMaxDelay(Duration expectedMaxDelay) {
            this.expectedMaxDelay = expectedMaxDelay;
            return this;
        }
    }
}