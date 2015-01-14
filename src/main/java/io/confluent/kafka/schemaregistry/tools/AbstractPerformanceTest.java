/**
 * Copyright 2015 Confluent Inc.
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
 **/

package io.confluent.kafka.schemaregistry.tools;

public abstract class AbstractPerformanceTest {
    private static final long NS_PER_MS = 1000000L;
    private static final long NS_PER_SEC = 1000 * NS_PER_MS;
    private static final long MIN_SLEEP_NS = 2 * NS_PER_MS;

    protected PerformanceStats stats;

    public AbstractPerformanceTest(long numEvents) {
        stats = new PerformanceStats(numEvents, 5000);
    }
    /**
     * The code that is executed once per iteration of the performance test.
     */
    protected abstract void doIteration(PerformanceStats.Callback cb);

    /** Returns true if the test has reached its completion criteria. */
    protected abstract boolean finished(int iteration);

    /** Returns true if the test is running slower than its target pace. */
    protected abstract boolean runningSlow(int iteration, float elapsed);

    protected void run(long iterationsPerSec) throws InterruptedException {
        long sleepTime = NS_PER_SEC / iterationsPerSec;
        long sleepDeficitNs = 0;
        long start = System.currentTimeMillis();
        for (int i = 0; !finished(i); i++) {
            long sendStart = System.currentTimeMillis();
            PerformanceStats.Callback cb = stats.nextCompletion(sendStart);
            doIteration(cb);

            /*
             * Maybe sleep a little to control throughput. Sleep time can be a bit inaccurate for times < 1 ms so
             * instead of sleeping each time instead wait until a minimum sleep time accumulates (the "sleep deficit")
             * and then make up the whole deficit in one longer sleep.
             */
            if (iterationsPerSec > 0) {
                float elapsed = (sendStart - start)/1000.f;
                if (elapsed > 0 && runningSlow(i, elapsed)) {
                    sleepDeficitNs += sleepTime;
                    if (sleepDeficitNs >= MIN_SLEEP_NS) {
                        long sleepMs = sleepDeficitNs / 1000000;
                        long sleepNs = sleepDeficitNs - sleepMs * 1000000;
                        Thread.sleep(sleepMs, (int) sleepNs);
                        sleepDeficitNs = 0;
                    }
                }
            }
        }

        /* print final results */
        stats.printTotal();
    }
}
