package com.jolira.gossip;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.net.InetSocketAddress;

import org.junit.Test;

/**
 * @author jfk
 * @date Mar 2, 2011 9:18:33 PM
 * @since 1.0
 * 
 */
public class MessageTest {
    /**
     * test the message class
     */
    @Test
    public void testMessage() {
        final Message msg = new Message("a", "b", "c");

        assertEquals("a", msg.getId());
        assertEquals("b", msg.getTopic());
        assertEquals("c", msg.getMessage());

        System.out.println(msg);

        final long sent = msg.sent();

        assertEquals(sent, msg.getSent());

        msg.addSentTo(null);

        final InetSocketAddress[] sentTo = msg.getSentTo();

        assertEquals(1, sentTo.length);
        assertNull(sentTo[0]);
    }
}
