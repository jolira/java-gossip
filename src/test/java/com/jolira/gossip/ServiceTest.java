/**
 * Copyright (c) 2011 jolira. All rights reserved. This program and the
 * accompanying materials are made available under the terms of the GNU Public
 * License 2.0 which is available at
 * http://www.gnu.org/licenses/old-licenses/gpl-2.0.html
 */

package com.jolira.gossip;

import static org.junit.Assert.assertEquals;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.concurrent.CountDownLatch;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author jfk
 * @date Mar 2, 2011 9:25:37 PM
 * @since 1.0
 * 
 */
public class ServiceTest {
    private static final int TREAD_COUNT = 3;

    final static Logger LOG = LoggerFactory.getLogger(ServiceTest.class);

    /**
     * Test the send functionality.
     * 
     * @throws SocketException
     * @throws UnknownHostException
     * @throws InterruptedException
     * 
     */
    @Test
    public void testService() throws SocketException, UnknownHostException, InterruptedException {
        final InetAddress localhost = InetAddress.getByName("localhost");
        final InetSocketAddress addr1 = new InetSocketAddress(localhost, 17685);
        final InetSocketAddress addr2 = new InetSocketAddress(localhost, 17684);
        final Service svc1 = new Service(addr1, new InetSocketAddress[] { addr2 });
        final Service svc2 = new Service(addr2, new InetSocketAddress[] { addr1, addr2 });
        final CountDownLatch latch = new CountDownLatch(2);

        svc1.add("topic.", new Listener() {
            @Override
            public void handleMessage(final String topic, final String message) {
                assertEquals("topic2", topic);
                assertEquals("test2", message.trim());
                latch.countDown();
            }
        });
        svc2.add("topic.", new Listener() {
            @Override
            public void handleMessage(final String topic, final String message) {
                assertEquals("topic1", topic);
                assertEquals("test1", message.trim());
                latch.countDown();
            }
        });
        svc1.send("topic1", "test1");
        svc2.send("topic2", "test2");
        latch.await();

        System.out.println(svc1);
        System.out.println(svc2);
    }

    /**
     * Lots more tests.
     * 
     * @throws SocketException
     * @throws UnknownHostException
     * @throws InterruptedException
     * 
     */
    // @Test
    public void testServiceMore() throws SocketException, UnknownHostException, InterruptedException {
        final InetAddress localhost = InetAddress.getByName("localhost");
        final Service[] services = new Service[TREAD_COUNT];
        final InetSocketAddress seed = new InetSocketAddress(localhost, 17686);
        final CountDownLatch startLatch = new CountDownLatch(TREAD_COUNT);
        final CountDownLatch endLatch = new CountDownLatch(TREAD_COUNT);

        for (int idx = 0; idx < TREAD_COUNT; idx++) {
            final InetSocketAddress addr = new InetSocketAddress(localhost, 17686 + idx);
            final Service svc = new Service(addr, new InetSocketAddress[] { seed });

            services[idx] = svc;

            svc.add("topic.", new Listener() {
                @Override
                public void handleMessage(final String topic, final String message) {
                    endLatch.countDown();

                    LOG.info("remaining count is " + endLatch.getCount());
                }
            });

            final Thread thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    startLatch.countDown();

                    try {
                        startLatch.await();
                    } catch (final InterruptedException e) {
                        throw new Error(e);
                    }

                    for (int m = 0; m < TREAD_COUNT; m++) {
                        svc.send("topic1", "test" + m);
                        try {
                            Thread.sleep(200);
                        } catch (final InterruptedException e) {
                            throw new Error(e);
                        }
                    }
                }
            });

            thread.setName("test-" + idx);
            thread.setDaemon(true);
            thread.start();
        }

        endLatch.await();
        System.out.println(services);
    }
}
