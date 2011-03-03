/**
 * Copyright (c) 2011 jolira. All rights reserved. This program and the
 * accompanying materials are made available under the terms of the GNU Public
 * License 2.0 which is available at
 * http://www.gnu.org/licenses/old-licenses/gpl-2.0.html
 */

package com.jolira.gossip;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedList;

/**
 * A message to be send to the peers.
 * 
 * @author jfk
 * @date Mar 2, 2011 1:53:44 PM
 * @since 1.0
 * 
 */
final class Message {
    private final String id;
    private final String topic;
    private final String message;
    private final Collection<InetSocketAddress> sentTo = new LinkedList<InetSocketAddress>();
    private long sent = -1;

    Message(final String id, final String topic, final String message) {
        this.id = id;
        this.message = message;
        this.topic = topic;
    }

    void addSentTo(final InetSocketAddress target) {
        sentTo.add(target);
    }

    /**
     * @return the id
     */
    public final String getId() {
        return id;
    }

    /**
     * @return the message
     */
    public final String getMessage() {
        return message;
    }

    public long getSent() {
        return sent;
    }

    /**
     * @return the sentTo
     */
    public final InetSocketAddress[] getSentTo() {
        final int size = sentTo.size();

        return sentTo.toArray(new InetSocketAddress[size]);
    }

    public String getTopic() {
        return topic;
    }

    public void resend() {
        sent = -1;
    }

    long sent() {
        return sent = System.currentTimeMillis();
    }

    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();

        builder.append("Message [id=");
        builder.append(id);
        builder.append(", topic=");
        builder.append(topic);
        builder.append(", message=");
        builder.append(message);
        builder.append(", sentTo=");
        builder.append(sentTo);

        if (sent != -1) {
            builder.append(", sent=");
            builder.append(new Date(sent));
            builder.append("]");
        }

        return builder.toString();
    }
}
