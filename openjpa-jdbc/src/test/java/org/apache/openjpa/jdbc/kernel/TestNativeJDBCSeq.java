/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.openjpa.jdbc.kernel;

import org.apache.openjpa.jdbc.conf.JDBCConfigurationImpl;
import org.apache.openjpa.lib.conf.Configuration;
import org.apache.openjpa.lib.jdbc.DelegatingConnection;
import org.apache.openjpa.lib.jdbc.DelegatingPreparedStatement;
import org.apache.openjpa.lib.jdbc.DelegatingResultSet;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class TestNativeJDBCSeq {

    @Test
    public void testCancelNoTimeout() {

        NativeJDBCSeq seq = new NativeJDBCSeq();
        Configuration c = new JDBCConfigurationImpl();
        seq.setConfiguration(c);
        Statement fake = new DelegatingPreparedStatement(null, null);
        try (NativeJDBCSeq.AC ignored = seq.enrollForCancel(fake)) {
            Assert.assertTrue(NativeJDBCSeq.cancelQueue.isEmpty());
        }

    }

    @Test
    public void testCancelTimeout() throws Exception {

        Map<String, String> p = new HashMap<>();
        p.put("openjpa.SequenceTimeout", "100");
        Configuration c = new JDBCConfigurationImpl();
        c.fromProperties(p);
        NativeJDBCSeq seq = new NativeJDBCSeq();
        seq.setConfiguration(c);
        Semaphore s = new Semaphore(0);
        Statement fake = new DelegatingPreparedStatement(null, null) {
            @Override
            public void cancel() {
                s.release();
            }
        };
        long before = System.nanoTime();
        try (NativeJDBCSeq.AC ignored = seq.enrollForCancel(fake)) {
            s.tryAcquire(1000, TimeUnit.MILLISECONDS);
        }
        long after = System.nanoTime();
        Assert.assertTrue((after - before) >= 100000000);

    }

    @Test
    public void testCancelGetSequence() throws Exception {

        long before = System.nanoTime();
        long r = sequenceTest(null);
        Assert.assertEquals(123, r);
        Assert.assertTrue(System.nanoTime() - before >= 500000000);

        before = System.nanoTime();
        r = sequenceTest(1000);
        Assert.assertEquals(123, r);
        Assert.assertTrue(System.nanoTime() - before >= 500000000);

        before = System.nanoTime();
        try {
            sequenceTest(100);
            Assert.fail("Must not have succeeded");
        } catch (SQLException e) {
            Assert.assertEquals("interrupted", e.getMessage());
        }
        long spent = (System.nanoTime() - before) / 1000000;
        Assert.assertTrue(String.valueOf(spent), spent >= 100);
        Assert.assertTrue(String.valueOf(spent), spent < 200);

    }

    private long sequenceTest(Integer configured) throws Exception {

        Map<String, String> p = new HashMap<>();
        if (configured != null) {
            p.put("openjpa.SequenceTimeout", configured.toString());
        }
        Configuration c = new JDBCConfigurationImpl();
        c.fromProperties(p);
        NativeJDBCSeq seq = new NativeJDBCSeq();
        seq.setConfiguration(c);

        AtomicBoolean interrupted = new AtomicBoolean(false);

        Connection conn = new DelegatingConnection(null) {
            @Override
            public PreparedStatement prepareStatement(String str) {
                return new DelegatingPreparedStatement(null, null) {
                    @Override
                    public void cancel() {
                        synchronized (interrupted) {
                            interrupted.set(true);
                            interrupted.notifyAll();
                        }
                    }

                    @Override
                    public void close() {
                    }

                    @Override
                    public ResultSet executeQuery() throws SQLException {
                        try {
                            Instant target = Instant.now().plus(Duration.ofMillis(500));
                            boolean wasInterrupted;
                            synchronized (interrupted) {
                                while (true) {
                                    wasInterrupted = interrupted.get();
                                    if (wasInterrupted) {
                                        System.out.println(Instant.now() + " interrupted");
                                        break;
                                    }
                                    long msLeft = Duration.between(Instant.now(), target).toMillis();
                                    if (msLeft <= 0) {
                                        System.out.println(Instant.now() + " timed out");
                                        break;
                                    }
                                    System.out.println(Instant.now() + " waiting for "+msLeft+"ms");
                                    interrupted.wait(msLeft);
                                }
                            }
                            if (wasInterrupted) {
                                // we were interrupted, pretend we are angry.
                                throw new SQLException("interrupted");
                            }
                            return new DelegatingResultSet(null, null, true) {
                                @Override
                                public boolean next() {
                                    return true;
                                }

                                @Override
                                public long getLong(int a) {
                                    return 123;
                                }

                                @Override
                                public void close() {
                                }
                            };
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                };
            }
        };

        return seq.getSequence(conn);

    }

    @Test
    public void testCancelWithdrawn() throws Exception {

        Map<String, String> p = new HashMap<>();
        p.put("openjpa.SequenceTimeout", "100");
        Configuration c = new JDBCConfigurationImpl();
        c.fromProperties(p);
        NativeJDBCSeq seq = new NativeJDBCSeq();
        seq.setConfiguration(c);
        Semaphore s = new Semaphore(0);
        Statement fake = new DelegatingPreparedStatement(null, null) {
            @Override
            public void cancel() {
                s.release();
            }
        };
        try (NativeJDBCSeq.AC ignored = seq.enrollForCancel(fake)) {
        }

        Assert.assertFalse(s.tryAcquire(500, TimeUnit.MILLISECONDS));

    }

    @Test
    public void testCancelOrder() throws Exception {

        Configuration c = new JDBCConfigurationImpl();
        NativeJDBCSeq seq = new NativeJDBCSeq();
        seq.setConfiguration(c);

        // times must be sorted.
        int [] times = { 50, 150, 300 };
        int count = times.length;

        BitSet b = BitSet.valueOf(new long[]{count});
        int w = b.length();
        List<int[]> orders = new ArrayList<>();
        for (int i=1; ; i++) {
            b = new BitSet(w*count);
            b.or(BitSet.valueOf(new long[]{i}));
            if (b.length() > w*count) { break; }
            Set<Integer> used = new HashSet<>();
            int [] item = new int[count];
            for (int j = 0; j<count; j++) {
                int bitsLeft = b.length();
                BitSet eval = b.get(0, Math.min(w, bitsLeft));
                int v;
                if (eval.isEmpty()) {
                    v = 0;
                } else {
                    v = (int)eval.toLongArray()[0];
                }
                if (v >= count || used.contains(v)) {
                    break;
                }
                used.add(v);
                item[j] = v;

                if (j == count-1) {
                    orders.add(item);
                } else {
                    if (bitsLeft > w) {
                        b = b.get(w, bitsLeft);
                    } else {
                        b = new BitSet();
                    }
                }

            }
        }

        for (int[] order : orders) {
            System.out.println(Arrays.toString(order));
            Semaphore s = new Semaphore(0);
            List<Long> result = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                int ts = times[order[i]];
                Statement fake = testStatement(s, ts, result);
                seq.enrollForCancel(fake, ts);
            }

            Assert.assertTrue(s.tryAcquire(count, 1, TimeUnit.SECONDS));

            for (int i=0; i<count; i++) {
                Assert.assertEquals(Long.valueOf(times[i]), result.get(i));
            }

        }

    }

    private Statement testStatement(Semaphore release, long timeout, List<Long> report) {

        return new DelegatingPreparedStatement(null, null) {
            @Override
            public void cancel() {
                release.release();
                report.add(timeout);
            }
        };

    }

}
