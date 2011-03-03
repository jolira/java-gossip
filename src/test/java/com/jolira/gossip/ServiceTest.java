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

/**
 * @author jfk
 * @date Mar 2, 2011 9:25:37 PM
 * @since 1.0
 * 
 */
public class ServiceTest {

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
        final InetAddress localhost = InetAddress.getLocalHost();
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
}
