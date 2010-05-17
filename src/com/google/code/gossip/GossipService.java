package com.google.code.gossip;

import java.io.IOException;
import java.io.PrintStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.management.Notification;
import javax.management.NotificationListener;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.sun.xml.internal.ws.util.ByteArrayBuffer;

/**
 * This object represents the service which is responsible for gossiping with other gossip members.
 * 
 * @author joshclemm, harmenw
 */
public class GossipService extends Thread implements NotificationListener {
	
	/** The maximal number of bytes the packet with the GOSSIP may be. (Default is 100 kb) */
	private static final int MAX_PACKET_SIZE = 102400;
	
	/** A instance variable holding the log level. */
	private int _logLevel = LogLevel.INFO;
	
	/** The list of members which are in the gossip group (not including myself). */
	private ArrayList<LocalGossipMember> _memberList;

	/** The list of members which are known to be dead. */
	private ArrayList<LocalGossipMember> _deadList;
	
	/** The settings for gossiping. */
	private GossipSettings _settings;

	/** The Random used for choosing a member to gossip with. */
	private Random _random;

	/** The socket used for the passive thread of the gossip service. */
	private DatagramSocket _server;

	/** The member I am representing. */
	private LocalGossipMember _me;
	
	/** A boolean whether the gossip service should keep running. */
	private AtomicBoolean _gossipServiceRunning;

	/** A ExecutorService used for executing the active and passive gossip threads. */
	private ExecutorService _gossipThreadExecutor;
	
	/**
	 * Constructor with the default settings.
	 * @throws InterruptedException
	 * @throws UnknownHostException
	 */
	public GossipService(StartupSettings startupSettings) throws InterruptedException, UnknownHostException {
		this(startupSettings.getPort(), startupSettings.getLogLevel(), startupSettings.getGossipMembers(), startupSettings.getGossipSettings());
	}

	/**
	 * Setup the client's lists, gossiping parameters, and parse the startup config file.
	 * @throws SocketException
	 * @throws InterruptedException
	 * @throws UnknownHostException
	 */
	public GossipService(int port, int logLevel, ArrayList<GossipMember> gossipMembers, GossipSettings settings) throws InterruptedException, UnknownHostException {

		// Set the logging level.
		_logLevel = logLevel;
		
		// Set the boolean for running the gossip service to true.
		_gossipServiceRunning = new AtomicBoolean(true);
		
		// Assign the GossipSettings to the instance variable.
		_settings = settings;
		
		// Add a shutdown hook so we can see when the service has been shutdown.
		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			public void run() {
				info("Service has been shutdown...");
			}
		}));

		// Initialize the gossip members list.
		_memberList = new ArrayList<LocalGossipMember>();
		
		// Initialize the dead gossip members list.
		_deadList = new ArrayList<LocalGossipMember>();

		// Initialize the random used for deciding on which gossip member to gossip with.
		_random = new Random();

		// Retrieve my address to look it up in the startup members list.
		String myIpAddress = InetAddress.getLocalHost().getHostAddress();

		// Create the local gossip member which I am representing.
		_me = new LocalGossipMember(myIpAddress, port, 0, this, settings.getCleanupInterval());
		
		// Print the startup member list when the service is in debug mode.
		debug("Startup member list:");
		debug("---------------------");
		// First print out myself.
		debug(_me);
		// Copy the list with members given to the local member list and print the member when in debug mode.
		for (GossipMember startupMember : gossipMembers) {
			if (!startupMember.equals(_me)) {
				LocalGossipMember member = new LocalGossipMember(startupMember.getHost(), startupMember.getPort(), 0, this, settings.getCleanupInterval());
				_memberList.add(member);
				debug(member);
			} else {
				info("Found myself in the members section of the configuration, you should not add the host itself to the members section.");
			}
		}
		
		// Start the service on the given port number.
		try {
			_server = new DatagramSocket(_me.getPort());
			
			// The server successfully started on the current port.
			info("Gossip service successfully initialized on port " + _me.getPort());
			debug("I am " + _me);
		} catch (SocketException ex) {
			// The port is probably already in use.
			_server = null;
			// Let's communicate this to the user.
			error("Error while starting the gossip service on port " + _me.getPort() + ": " + ex.getMessage());
			System.exit(-1);
		}
	}
	
	private void error(Object message) {
		if (_logLevel >= LogLevel.ERROR) printMessage(message, System.err);
	}
	
	private void info(Object message) {
		if (_logLevel >= LogLevel.INFO) printMessage(message, System.out);
	}
	
	private void debug(Object message) {
		if (_logLevel >= LogLevel.DEBUG) printMessage(message, System.out);
	}
	
	private void printMessage(Object message, PrintStream out) {
		String addressString = "unknown";
		if (_me != null)
			addressString = _me.getAddress();
		out.println("[" + addressString + "][" + new Date().toString() + "] " + message);
	}

	/**
	 * Performs the sending of the membership list, after we have
	 * incremented our own heartbeat.
	 */
	private void sendMembershipList() {
		debug("Send sendMembershipList() is called.");

		// Increase the heartbeat of myself by 1.
		_me.setHeartbeat(_me.getHeartbeat() + 1);
		
		// Print my heartbeat every 5 seconds.
		if (_logLevel != LogLevel.DEBUG && _me.getHeartbeat() % ( (1000 * 5) / _settings.getGossipInterval()) == 0)
			info("My heartbeat is currently " + _me.getHeartbeat());

		synchronized (_memberList) {
			try {
				LocalGossipMember member = selectPartner();

				if (member != null) {
					InetAddress dest = InetAddress.getByName(member.getHost());
					
					// Create a StringBuffer for the JSON message.
					JSONArray jsonArray = new JSONArray();
					debug("Sending memberlist to " + dest + ":" + member.getPort());
					debug("---------------------");
					
					// First write myself, append the JSON representation of the member to the buffer.
					jsonArray.put(_me.toJSONObject());
					debug(_me);
					
					// Then write the others.
					for (int i=0; i<_memberList.size(); i++) {
						LocalGossipMember other = _memberList.get(i);
						// Append the JSON representation of the member to the buffer.
						jsonArray.put(other.toJSONObject());
						debug(other);
					}
					debug("---------------------");
					
					// Write the objects to a byte array.
					byte[] json_bytes = jsonArray.toString().getBytes();
					
					int packet_length = json_bytes.length;
					
					if (packet_length < MAX_PACKET_SIZE) {
						
						// Convert the packet length to the byte representation of the int.
						byte[] length_bytes = new byte[4];
						length_bytes[0] =(byte)(  packet_length >> 24 );
						length_bytes[1] =(byte)( (packet_length << 8) >> 24 );
						length_bytes[2] =(byte)( (packet_length << 16) >> 24 );
						length_bytes[3] =(byte)( (packet_length << 24) >> 24 );
						
						
						debug("Sending message ("+packet_length+" bytes): " + jsonArray.toString());
						
						ByteArrayBuffer byteBuffer = new ByteArrayBuffer();
						// Write the first 4 bytes with the length of the rest of the packet.
						byteBuffer.write(length_bytes);
						// Write the json data.
						byteBuffer.write(json_bytes);
						
						byte[] buf = byteBuffer.getRawData();
						
						DatagramSocket socket = new DatagramSocket();
						DatagramPacket datagramPacket = new DatagramPacket(buf, buf.length, dest, member.getPort());
						socket.send(datagramPacket);
						socket.close();
					} else {
						error("The length of the to be send message is too large (" + packet_length + " > " + MAX_PACKET_SIZE + ").");
					}
				}

			} catch (IOException e1) {
				e1.printStackTrace();
			}
		}
	}

	/**
	 * [The selectToSend() function.]
	 * Find a random peer from the local membership list.
	 * In the case where this client is the only member in the list, this method will return null.
	 * @return Member random member if list is greater than 1, null otherwise
	 */
	private LocalGossipMember selectPartner() {
		LocalGossipMember member = null;

		// We can only send a message if there are actually other members.
		if (_memberList.size() > 0) {
			// Get the index of the random member.
			int randomNeighborIndex = _random.nextInt(_memberList.size());
			member = _memberList.get(randomNeighborIndex);
		} else {
			debug("I am alone in this world.");
		}

		return member;
	}

	/**
	 * [The active thread: periodically send gossip request.]
	 * The class handles gossiping the membership list.
	 * This information is important to maintaining a common
	 * state among all the nodes, and is important for detecting
	 * failures.
	 */
	private class ActiveGossipThread implements Runnable {

		private AtomicBoolean _keepRunning;

		public ActiveGossipThread() {
			_keepRunning = new AtomicBoolean(true);
		}

		@Override
		public void run() {
			while(_keepRunning.get()) {
				try {
					TimeUnit.MILLISECONDS.sleep(_settings.getGossipInterval());
					sendMembershipList();
				} catch (InterruptedException e) {
					// This membership thread was interrupted externally, shutdown
					debug("The ActiveGossipThread was interrupted externally, shutdown.");
					e.printStackTrace();
					_keepRunning.set(false);
				}
			}

			_keepRunning = null;
		}
	}

	/**
	 * [The passive thread: reply to incoming gossip request.]
	 * This class handles the passive cycle, where this client
	 * has received an incoming message.  For now, this message
	 * is always the membership list, but if you choose to gossip
	 * additional information, you will need some logic to determine
	 * the incoming message.
	 */
	private class PassiveGossipThread implements Runnable {

		private AtomicBoolean _keepRunning;

		public PassiveGossipThread() {
			_keepRunning = new AtomicBoolean(true);
		}

		@Override
		public void run() {
			while(_keepRunning.get()) {
				try {
					// Create a byte array with the size of the buffer.
					byte[] buf = new byte[_server.getReceiveBufferSize()];
					DatagramPacket p = new DatagramPacket(buf, buf.length);
					_server.receive(p);
					debug("A message has been received from " + p.getAddress() + ":" + p.getPort() + ".");
					
			        int packet_length = 0;
			        for (int i = 0; i < 4; i++) {
			            int shift = (4 - 1 - i) * 8;
			            packet_length += (buf[i] & 0x000000FF) << shift;
			        }
			        
			        // Check whether the package is smaller than the maximal packet length.
			        // A package larger than this would not be possible to be send from a GossipService,
			        // since this is check before sending the message.
			        // This could normally only occur when the list of members is very big,
			        // or when the packet is misformed, and the first 4 bytes is not the right in anymore.
			        // For this reason we regards the message.
			        if (packet_length <= MAX_PACKET_SIZE) {
			        
				        byte[] json_bytes = new byte[packet_length];
				        for (int i=0; i<packet_length; i++) {
				        	json_bytes[i] = buf[i+4];
				        }
	
						// Extract the members out of the packet
						String receivedMessage = new String(json_bytes);
						debug("Received message (" + packet_length + " bytes): " + receivedMessage);
						
						try {
							
							ArrayList<GossipMember> remoteGossipMembers = new ArrayList<GossipMember>();
							
							debug("Received member list:");
							// Convert the received JSON message to a JSON array.
							JSONArray jsonArray = new JSONArray(receivedMessage);
							// The JSON array should contain all members.
							// Let's iterate over them.
							for (int i = 0; i < jsonArray.length(); i++) {
								JSONObject memberJSONObject = jsonArray.getJSONObject(i);
								// Now the array should contain 3 objects (hostname, port and heartbeat).
								if (memberJSONObject.length() == 3) {
									// Ok, now let's create the member object.
									RemoteGossipMember member = new RemoteGossipMember(memberJSONObject.getString(GossipMember.JSON_HOST), memberJSONObject.getInt(GossipMember.JSON_PORT), memberJSONObject.getInt(GossipMember.JSON_HEARTBEAT));
									debug(member.toString());
									remoteGossipMembers.add(member);
								} else {
									error("The received member object does not contain 3 objects:\n" + memberJSONObject.toString());
								}
								
							}
							
							// Merge our list with the one we just received
							mergeLists(remoteGossipMembers);
							
						} catch (JSONException e) {
							error("The received message is not well-formed JSON. The following message has been dropped:\n" + receivedMessage);
						}
					
			        } else {
			        	error("The received message is not of the expected size, it has been dropped.");
			        }

				} catch (IOException e) {
					e.printStackTrace();
					_keepRunning.set(false);
				}
			}
		}
		
		/**
		 * Merge remote list (received from peer), and our local member list.
		 * Simply, we must update the heartbeats that the remote list has with
		 * our list.  Also, some additional logic is needed to make sure we have 
		 * not timed out a member and then immediately received a list with that 
		 * member.
		 * @param remoteList
		 */
		private void mergeLists(ArrayList<GossipMember> remoteList) {

			synchronized (GossipService.this._deadList) {

				synchronized (GossipService.this._memberList) {

					for (GossipMember remoteMember : remoteList) {
						// Skip myself. We don't want ourselves in the local member list.
						if (!remoteMember.equals(_me)) {
							if (GossipService.this._memberList.contains(remoteMember)) {
								debug("The local list already contains the remote member (" + remoteMember + ").");
								// The local memberlist contains the remote member.
								LocalGossipMember localMember = GossipService.this._memberList.get(GossipService.this._memberList.indexOf(remoteMember));
	
								// Let's synchronize it's heartbeat.
								if (remoteMember.getHeartbeat() > localMember.getHeartbeat()) {
									// update local list with latest heartbeat
									localMember.setHeartbeat(remoteMember.getHeartbeat());
									// and reset the timeout of that member
									localMember.resetTimeoutTimer();
								}
								// TODO: Otherwise, should we inform the other when the heartbeat is already higher?
							} else {
								// The local list does not contain the remote member.
								debug("The local list does not contain the remote member (" + remoteMember + ").");
	
								// The remote member is either brand new, or a previously declared dead member.
								// If its dead, check the heartbeat because it may have come back from the dead.
								if (GossipService.this._deadList.contains(remoteMember)) {
									// The remote member is known here as a dead member.
									debug("The remote member is known here as a dead member.");
									LocalGossipMember localDeadMember = GossipService.this._deadList.get(GossipService.this._deadList.indexOf(remoteMember));
									// If a member is restarted the heartbeat will restart from 1, so we should check that here.
									// So a member can become from the dead when it is either larger than a previous heartbeat (due to network failure)
									// or when the heartbeat is 1 (after a restart of the service).
									// TODO: What if the first message of a gossip service is sent to a dead node? The second member will receive a heartbeat of two.
									// TODO: The above does happen. Maybe a special message for a revived member?
									// TODO: Or maybe when a member is declared dead for more than _settings.getCleanupInterval() ms, reset the heartbeat to 0.
									// It will then accept a revived member.
									// The above is now handle by checking whether the heartbeat differs _settings.getCleanupInterval(), it must be restarted.
									if (remoteMember.getHeartbeat() == 1 
											|| ((localDeadMember.getHeartbeat() - remoteMember.getHeartbeat()) * -1) >  (_settings.getCleanupInterval() / 1000)
											|| remoteMember.getHeartbeat() > localDeadMember.getHeartbeat()) {
										debug("The remote member is back from the dead. We will remove it from the dead list and add it as a new member.");
										// The remote member is back from the dead.
										// Remove it from the dead list.
										GossipService.this._deadList.remove(localDeadMember);
										// Add it as a new member and add it to the member list.
										LocalGossipMember newLocalMember = new LocalGossipMember(remoteMember.getHost(), remoteMember.getPort(), remoteMember.getHeartbeat(), GossipService.this, _settings.getCleanupInterval());
										GossipService.this._memberList.add(newLocalMember);
										newLocalMember.startTimeoutTimer();
										info("Removed remote member " + remoteMember.getAddress() + " from dead list and added to local member list.");
									}
								} else {
									// Brand spanking new member - welcome.
									LocalGossipMember newLocalMember = new LocalGossipMember(remoteMember.getHost(), remoteMember.getPort(), remoteMember.getHeartbeat(), GossipService.this, _settings.getCleanupInterval());
									GossipService.this._memberList.add(newLocalMember);
									newLocalMember.startTimeoutTimer();
									info("Added new remote member " + remoteMember.getAddress() + " to local member list.");
								}
							}
						}
					}
				}
			}
		}
	}

	/**
	 * Starts the client.  Specifically, start the various cycles for this protocol.
	 * Start the gossip thread and start the receiver thread.
	 * @throws InterruptedException
	 */
	public void run() {
		// Start all timers except for me
		for (LocalGossipMember member : _memberList) {
			if (member != _me) {
				member.startTimeoutTimer();
			}
		}

		_gossipThreadExecutor = Executors.newCachedThreadPool();
		//  The receiver thread is a passive player that handles
		//  merging incoming membership lists from other neighbors.
		_gossipThreadExecutor.execute(new PassiveGossipThread());
		//  The gossiper thread is an active player that 
		//  selects a neighbor to share its membership list
		_gossipThreadExecutor.execute(new ActiveGossipThread());

		// Potentially, you could kick off more threads here
		//  that could perform additional data synching
		
		info("The GossipService is started.");

		// keep the main thread around
		while(_gossipServiceRunning.get()) {
			try {
				TimeUnit.SECONDS.sleep(10);
			} catch (InterruptedException e) {
				info("The GossipClient was interrupted.");
			}
		}
	}

	/**
	 * Shutdown the gossip service.
	 */
	public void shutdown() {
		_gossipThreadExecutor.shutdown();
		_gossipServiceRunning.set(false);
	}
	
	/**
	 * All timers associated with a member will trigger this method when it goes
	 * off. The timer will go off if we have not heard from this member in
	 * <code> _settings.T_CLEANUP </code> time.
	 */
	@Override
	public void handleNotification(Notification notification, Object handback) {

		// Get the local gossip member associated with the notification.
		LocalGossipMember deadMember = (LocalGossipMember) notification.getUserData();

		info("Dead member detected: " + deadMember);

		// Remove the member from the active member list.
		synchronized (this._memberList) {
			this._memberList.remove(deadMember);
		}

		// Add the member to the dead member list.
		synchronized (this._deadList) {
			this._deadList.add(deadMember);
		}
	}
}
