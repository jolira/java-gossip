package com.google.code.gossip;

import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.code.gossip.manager.GossipManager;
import com.google.code.gossip.manager.random.RandomGossipManager;

/**
 * This object represents the service which is responsible for gossiping with
 * other gossip members.
 * 
 * @author joshclemm, harmenw
 */
public class GossipService {
    private final static Logger LOG = LoggerFactory.getLogger(GossipService.class);

    public static void debug(final Object message) {
        final String msg = message.toString();

        LOG.debug(msg);
    }

    public static void error(final Object message) {
        final String msg = message.toString();

        LOG.error(msg);
    }

    public static void info(final Object message) {
        final String msg = message.toString();

        LOG.info(msg);
    }

    private final GossipManager _gossipManager;

    /**
     * Constructor with the default settings.
     * 
     * @throws InterruptedException
     * @throws UnknownHostException
     */
    public GossipService(final StartupSettings startupSettings) throws InterruptedException, UnknownHostException {
        this(InetAddress.getLocalHost().getHostAddress(), startupSettings.getPort(), startupSettings.getLogLevel(),
                startupSettings.getGossipMembers(), startupSettings.getGossipSettings());
    }

    /**
     * Setup the client's lists, gossiping parameters, and parse the startup
     * config file.
     * 
     * @throws SocketException
     * @throws InterruptedException
     * @throws UnknownHostException
     */
    public GossipService(final String ipAddress, final int port, final int logLevel,
            final ArrayList<GossipMember> gossipMembers, final GossipSettings settings) throws InterruptedException,
            UnknownHostException {
        _gossipManager = new RandomGossipManager(ipAddress, port, settings, gossipMembers);
    }

    public void shutdown() {
        _gossipManager.shutdown();
    }

    public void start() {
        _gossipManager.start();
    }
}
