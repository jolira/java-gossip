/**
 * Copyright (c) 2011 jolira. All rights reserved. This program and the
 * accompanying materials are made available under the terms of the GNU Public
 * License 2.0 which is available at
 * http://www.gnu.org/licenses/old-licenses/gpl-2.0.html
 */

package com.jolira.gossip;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Reader;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple Gossip-inspires service.
 * 
 * @author jfk
 * @date Mar 2, 2011 12:20:05 PM
 * @since 1.0
 * 
 */
public class Service {
    static final Logger LOG = LoggerFactory.getLogger(Service.class);
    private static final String HEARTBEAT_TOPIC = ":HEARTBEAT:";
    private final static String ACK_TOPIC = ":ACK:";
    private static final int MAX_SIZE = 8 * 65536;
    private static final long HEARTBEAT = 5000;

    private static String makeID() {
        final UUID uuid = UUID.randomUUID();

        return uuid.toString();
    }

    private static InetSocketAddress[] parsePeers(final String _peers) {
        try {
            return unmarshalPeers(_peers);
        } catch (final UnknownHostException e) {
            LOG.error("error parsing " + e);
        }

        return new InetSocketAddress[] {};
    }

    private static InetSocketAddress[] unmarshalPeers(final String peers) throws UnknownHostException {
        final Collection<InetSocketAddress> result = new LinkedList<InetSocketAddress>();
        final StringTokenizer izer = new StringTokenizer(peers, ";");

        while (izer.hasMoreTokens()) {
            final String token = izer.nextToken();
            final int pos = token.indexOf(':');

            if (pos == -1) {
                continue;
            }

            final String host = token.substring(0, pos);
            final String port = token.substring(pos + 1);
            final InetAddress remote = InetAddress.getByName(host);
            final int _port = Integer.parseInt(port);
            final InetSocketAddress socketAddr = new InetSocketAddress(remote, _port);

            result.add(socketAddr);
        }

        final int size = result.size();

        return result.toArray(new InetSocketAddress[size]);
    }

    private final Executor executor = Executors.newCachedThreadPool(new ThreadFactory() {
        private int number = 0;

        @Override
        public synchronized Thread newThread(final Runnable r) {
            final Thread thread = new Thread(r);
            final int no = number++;

            thread.setDaemon(true);
            thread.setName("gossip-" + no);

            return thread;
        }
    });
    private final Collection<InetSocketAddress> peers = new HashSet<InetSocketAddress>();
    private final Collection<InetSocketAddress> unconfirmed = new LinkedList<InetSocketAddress>();
    private final Collection<Message> pending = new LinkedList<Message>();
    private final Random random = new Random();
    private final DatagramSocket socket;
    private final Collection<InetSocketAddress> seeds;
    private long lastActivity = -1;
    private final String id = makeID();

    private final Map<Pattern, Listener> listenerByPattern = new TreeMap<Pattern, Listener>();

    /**
     * Create the service.
     * 
     * @param listener
     *            the socket to listen to
     * @param seeds
     *            the initial peers
     * @throws SocketException
     *             could not create the socket
     */
    public Service(final InetSocketAddress listener, final InetSocketAddress[] seeds) throws SocketException {
        this.seeds = Arrays.asList(seeds);

        socket = new DatagramSocket(listener);

        executor.execute(new Runnable() {
            @Override
            public void run() {
                processRemoteMessages();
            }
        });
        executor.execute(new Runnable() {
            @Override
            public void run() {
                heartbeat();
            }
        });
    }

    private void add(final InetSocketAddress remote) {
        synchronized (peers) {
            if (peers.add(remote)) {
                LOG.info("found first-class peer " + remote);
            }
        }
    }

    /**
     * Add a listener.
     * 
     * @param topic
     *            a regex that matches the topic to listen for
     * @param listener
     *            the listener
     */
    public void add(final String topic, final Listener listener) {
        final Pattern p = Pattern.compile(topic);

        synchronized (listenerByPattern) {
            listenerByPattern.put(p, listener);
        }
    }

    @SuppressWarnings("unchecked")
    private Entry<Pattern, Listener>[] getListeners() {
        synchronized (listenerByPattern) {
            final Collection<Entry<Pattern, Listener>> entries = listenerByPattern.entrySet();
            final int size = entries.size();

            return entries.toArray(new Entry[size]);
        }
    }

    private InetSocketAddress[] getPeers() {
        synchronized (peers) {
            final int size = peers.size();

            return peers.toArray(new InetSocketAddress[size]);
        }
    }

    private Message[] getPendingMessages() {
        synchronized (pending) {
            final int size = pending.size();

            return pending.toArray(new Message[size]);
        }
    }

    final void heartbeat() {
        final Thread current = Thread.currentThread();

        current.setName("gossip-heartbeat");

        for (;;) {
            if (current.isInterrupted()) {
                break;
            }

            final long now = System.currentTimeMillis();

            if (lastActivity + HEARTBEAT < now) {
                send(HEARTBEAT_TOPIC, null);
            }

            try {
                Thread.sleep(HEARTBEAT / 2);
            } catch (final InterruptedException e) {
                break;
            }
        }
    }

    private byte[] marshal(final Message message) {
        final ByteArrayOutputStream _out = new ByteArrayOutputStream();
        final PrintWriter out = new PrintWriter(_out);
        final String messageID = message.getId();
        final InetSocketAddress[] sentTo = message.getSentTo();
        final InetSocketAddress[] _peers = getPeers();
        final String msg = message.getMessage();
        final String topic = message.getTopic();

        out.println(id);
        marshal(out, _peers);
        out.println(messageID);
        marshal(out, sentTo);
        out.println(topic == null ? "" : topic);

        if (msg != null) {
            out.print(msg);
        }

        out.close();

        return _out.toByteArray();
    }

    private void marshal(final PrintWriter out, final InetSocketAddress[] sentTo) {
        for (final InetSocketAddress target : sentTo) {
            final InetAddress address = target.getAddress();
            final String hostname = address.getHostAddress();
            final int port = target.getPort();

            out.print(hostname);
            out.print(':');
            out.print(port);
            out.print(';');
        }

        out.println();
    }

    private boolean needsSending(final Message message) {
        final long sent = message.getSent();

        if (sent <= 0) {
            return true;
        }

        final long now = System.currentTimeMillis();

        return now > sent + HEARTBEAT;
    }

    final void process(final DatagramPacket packet) throws IOException {
        final InetSocketAddress remote = (InetSocketAddress) packet.getSocketAddress();
        final byte[] data = packet.getData();
        final ByteArrayInputStream _in = new ByteArrayInputStream(data);
        final BufferedReader reader = new BufferedReader(new InputStreamReader(_in));
        final String _id = reader.readLine();
        final String _peers = reader.readLine();
        final String messageID = reader.readLine();
        final String _sentTo = reader.readLine();
        final String topic = reader.readLine();
        final String msg = readMessage(reader);

        LOG.info("message received from " + remote + ", id=" + _id + ", peers=" + _peers + ", messageID=" + messageID
                + ", sentTo=" + _sentTo + ", topic=" + topic + ", msg=" + msg);

        if (id.equals(_id)) {
            resend(messageID);
            return;
        }

        processPeers(remote, _peers);

        if (ACK_TOPIC.equals(topic)) {
            processAck(remote, messageID);
            return;
        }

        send(ACK_TOPIC, null);

        final Entry<Pattern, Listener>[] listeners = getListeners();

        for (final Entry<Pattern, Listener> entry : listeners) {
            final Pattern pattern = entry.getKey();
            final Matcher matcher = pattern.matcher(topic);

            if (matcher.matches()) {
                final Listener listener = entry.getValue();

                listener.handleMessage(topic, msg);
            }
        }
    }

    private void processAck(final InetSocketAddress remote, final String messageID) {
        synchronized (pending) {
            for (final Message msg : pending) {
                final String msgID = msg.getId();

                if (messageID.equals(msgID)) {
                    pending.remove(msg);
                    break;
                }
            }
        }

        synchronized (unconfirmed) {
            unconfirmed.remove(remote);
        }
    }

    private void processPeers(final InetSocketAddress remote, final String _peers) {
        add(remote);

        final InetSocketAddress[] peers_ = parsePeers(_peers);

        for (final InetSocketAddress peer : peers_) {
            add(peer);
        }
    }

    final void processRemoteMessages() {
        while (true) {
            final Thread current = Thread.currentThread();

            if (current.isInterrupted()) {
                break;
            }

            final DatagramPacket packet = receive();

            executor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        process(packet);
                    } catch (final IOException e) {
                        LOG.error("failed to process message", e);
                    }
                }
            });
        }
    }

    private String readMessage(final Reader reader) throws IOException {
        final StringBuilder buf = new StringBuilder();

        for (;;) {
            final char[] c = new char[2048];
            final int read = reader.read(c);

            if (read == -1) {
                break;
            }

            buf.append(c);
        }

        return buf.toString();
    }

    private DatagramPacket receive() {
        final byte buf[] = new byte[MAX_SIZE];
        final DatagramPacket packet = new DatagramPacket(buf, buf.length);

        try {
            socket.receive(packet);
        } catch (final IOException e) {
            LOG.error("receive failed", e);
        }

        return packet;
    }

    private void resend(final String messageID) {
        LOG.info("resending message " + messageID + " as it was sent by self");

        synchronized (pending) {
            for (final Message msg : pending) {
                final String msgID = msg.getId();

                if (messageID.equals(msgID)) {
                    msg.resend();
                    break;
                }
            }
        }

        sendPendingMessages();
    }

    private InetSocketAddress selectPeer(final Message message) {
        final Set<InetSocketAddress> selectable = new HashSet<InetSocketAddress>();

        synchronized (peers) {
            selectable.addAll(peers);
        }
        synchronized (unconfirmed) {
            selectable.removeAll(unconfirmed);
        }

        final InetSocketAddress[] sentTo = message.getSentTo();
        final Collection<InetSocketAddress> _sentTo = Arrays.asList(sentTo);

        selectable.removeAll(_sentTo);

        final int size = selectable.size();

        if (size == 0) {
            return selectSeed(_sentTo);
        }

        final InetSocketAddress[] sockets = selectable.toArray(new InetSocketAddress[size]);

        return selectRandom(sockets);
    }

    private InetSocketAddress selectRandom(final InetSocketAddress[] sockets) {
        int pos;

        synchronized (random) {
            pos = random.nextInt(sockets.length);
        }

        return sockets[pos];
    }

    private InetSocketAddress selectSeed(final Collection<InetSocketAddress> _sentTo) {
        final Set<InetSocketAddress> selectable = new HashSet<InetSocketAddress>();

        selectable.addAll(seeds);
        selectable.removeAll(_sentTo);

        synchronized (unconfirmed) {
            selectable.removeAll(unconfirmed);
        }

        final int size = selectable.size();

        if (size == 0) {
            LOG.error("exhausted all seeds for " + this);
            return null;
        }

        final InetSocketAddress[] sockets = selectable.toArray(new InetSocketAddress[size]);

        return selectRandom(sockets);
    }

    /**
     * Send a message to all peers.
     * 
     * @param message
     *            the message to be sent to the client
     * @param topic
     *            the topic of the message
     */
    public void send(final String topic, final String message) {
        final String _id = makeID();
        final Message msg = new Message(_id, topic, message);

        synchronized (pending) {
            pending.add(msg);
        }

        executor.execute(new Runnable() {
            @Override
            public void run() {
                sendPendingMessages();
            }
        });
    }

    private void sendMessage(final Message message) {
        synchronized (message) {
            if (!needsSending(message)) {
                return;
            }

            final InetSocketAddress target = selectPeer(message);

            if (target == null) {
                return;
            }

            message.addSentTo(target);

            LOG.info("sending message " + message + " to " + target);

            final byte[] raw = marshal(message);

            try {
                final DatagramPacket packet = new DatagramPacket(raw, raw.length, target);

                socket.send(packet);
            } catch (final SocketException e) {
                LOG.error("socket exception while sending " + message, e);
            } catch (final IOException e) {
                LOG.error("I/O exception while sending " + message, e);
            } finally {
                message.sent();
            }
        }
    }

    final void sendPendingMessages() {
        final Message[] messages = getPendingMessages();

        try {
            for (final Message message : messages) {
                sendMessage(message);
            }
        } finally {
            lastActivity = System.currentTimeMillis();
        }
    }

    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();

        builder.append("Service [id=");
        builder.append(id);
        builder.append(", seeds=");
        builder.append(seeds);
        builder.append(", peers=");

        synchronized (peers) {
            builder.append(peers);
        }

        builder.append(", unconfirmed=");

        synchronized (unconfirmed) {
            builder.append(unconfirmed);
        }

        builder.append(", pending=");

        synchronized (pending) {
            builder.append(pending);
        }

        builder.append(", lastActivity=");
        builder.append(lastActivity);
        builder.append(", id=");
        builder.append(id);
        builder.append("]");

        return builder.toString();
    }
}
