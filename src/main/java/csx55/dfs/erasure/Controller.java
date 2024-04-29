// package csx55.dfs.erasure;

// import java.io.IOException;
// import java.net.ServerSocket;
// import java.util.ArrayList;
// import java.util.Collections;
// import java.util.HashMap;
// import java.util.List;
// import java.util.Map;
// import java.util.Scanner;
// import java.util.Timer;
// import java.util.TimerTask;
// import java.util.concurrent.ConcurrentHashMap;
// import java.util.stream.Collectors;

// import csx55.dfs.tcp.TCPConnection;
// import csx55.dfs.tcp.TCPServer;
// import csx55.dfs.utils.Node;
// import csx55.dfs.wireformats.Event;
// import csx55.dfs.wireformats.FetchChunkServers;
// import csx55.dfs.wireformats.FetchChunksList;
// import csx55.dfs.wireformats.FetchChunksListResponse;
// import csx55.dfs.wireformats.Protocol;
// import csx55.dfs.wireformats.Register;
// import csx55.dfs.wireformats.RegisterResponse;
// import csx55.dfs.wireformats.ReportChunkCorruption;
// import csx55.dfs.wireformats.ChunkServerList;
// import csx55.dfs.wireformats.ErrorCorrection;
// import csx55.dfs.wireformats.MajorHeartbeat;
// import csx55.dfs.wireformats.MinorHeartbeat;

// public class Controller implements Node {
// // Constants representing different commands

// private Map<String, TCPConnection> clientConnections = new HashMap<>();

// /*
// * stores reference to metadata for each chunkserver
// * this will be used to update the metadata by heartbeats
// */
// private final ConcurrentHashMap<String, TCPConnection> chunkServerConnections
// = new ConcurrentHashMap<>();

// /*
// * this stores location of chunks and its two replicas
// * will be used for retrieve chunk response to client
// */
// private final ConcurrentHashMap<String, String> chunkAndServerMap = new
// ConcurrentHashMap<>();

// public static void main(String[] args) {
// // Check if the port number is provided as a command-line argument
// if (args.length < 1) {
// System.out.println("Error starting the Registry. Usage: java
// csx55.overlay.node.Registry portnum");
// }

// Controller registry = new Controller();

// /*
// * defining serverSocket in try-with-resources statement ensures
// * that the serverSocket is closed after the block ends
// */
// try (ServerSocket serverSocket = new ServerSocket(Integer.valueOf(args[0])))
// {
// /*
// * start the server thread after initializing the server socket
// * invoke start function to start a new thread execution(invoking run() is not
// * the right way)
// */
// (new Thread(new TCPServer(registry, serverSocket))).start();

// // Take commands from console
// registry.takeCommands();
// } catch (IOException e) {
// System.out.println(e.getMessage());
// e.printStackTrace();
// }
// }

// /* Takes user commands from console */
// private void takeCommands() {
// System.out
// .println("This is the Controller command console. Please enter a valid
// command to start the overlay.");

// try (Scanner scan = new Scanner(System.in)) {
// while (true) {
// String line = scan.nextLine().toLowerCase();
// String[] input = line.split("\\s+");
// switch (input[0]) {

// default:
// System.out.println("Please enter a valid command! Options are:\n" +
// " - peer-nodes\n");
// break;
// }
// }
// }
// }

// public void handleIncomingEvent(Event event, TCPConnection connection) {
// switch (event.getType()) {
// case Protocol.CLIENT_REGISTER_REQUEST:
// handleClientRegistration((Register) event, connection);
// break;

// case Protocol.CHUNK_SERVER_REGISTER_REQUEST:
// handleChunkServerRegistration((Register) event, connection);
// break;

// case Protocol.DEREGISTER_REQUEST:
// handleDeregistrationEvent((Register) event, connection);
// break;

// case Protocol.FETCH_CHUNK_SERVERS:
// sendChunkServers((FetchChunkServers) event, connection);
// break;

// case Protocol.FETCH_CHUNKS:
// sendChunks((FetchChunksList) event, connection);
// break;

// }
// }

// private synchronized void handleClientRegistration(Register registerEvent,
// TCPConnection connection) {
// // typecast event object to Register
// String nodes = registerEvent.getConnectionReadable();
// String ipAddress = connection.getSocket().getInetAddress().getHostAddress();

// boolean hasMismatch = checkMismatch(nodes, ipAddress);
// byte status;
// String message;

// /* TODO: detect identifier collision */
// if (!hasMismatch) {
// /* validate peer id */

// clientConnections.put(nodes, connection);

// message = "Registration request successful. The number of clients currently
// is ("
// + clientConnections.size() + ").\n";
// status = Protocol.SUCCESS;
// System.out.println("Connected Node: " + nodes);

// } else {
// message = "Unable to process request. Responding with a failure while
// mismatch is" + hasMismatch;
// System.out.println(message);
// status = Protocol.FAILURE;
// }
// RegisterResponse response = new RegisterResponse(status, message);
// try {
// connection.getTCPSenderThread().sendData(response.getBytes());
// } catch (IOException | InterruptedException e) {
// System.out.println(e.getMessage());
// clientConnections.remove(nodes);
// e.printStackTrace();
// }

// }

// private synchronized void handleChunkServerRegistration(Register
// registerEvent, TCPConnection connection) {
// // typecast event object to Register
// String nodes = registerEvent.getConnectionReadable();
// String ipAddress = connection.getSocket().getInetAddress().getHostAddress();

// boolean hasMismatch = checkMismatch(nodes, ipAddress);
// byte status;
// String message;

// /* TODO: detect identifier collision */
// if (!hasMismatch) {
// /* validate peer id */

// chunkServerConnections.put(nodes, connection);

// message = "Registration request successful. The number of chunk servers
// currently is ("
// + chunkServerConnections.size() + ").\n";
// status = Protocol.SUCCESS;

// System.out.println("Connected Node: " + nodes);

// } else {
// message = "Unable to process request. Responding with a failure while
// mismatch is" + hasMismatch;
// System.out.println(message);
// status = Protocol.FAILURE;
// }
// RegisterResponse response = new RegisterResponse(status, message);
// try {
// connection.getTCPSenderThread().sendData(response.getBytes());
// } catch (IOException | InterruptedException e) {
// System.out.println(e.getMessage());
// chunkServerConnections.remove(nodes);
// e.printStackTrace();
// }

// }

// /* TODO: handle exit of chunkserver and client */
// private synchronized void handleDeregistrationEvent(Register registerEvent,
// TCPConnection connection) {
// // // typecast event object to Register
// // String nodes = registerEvent.getConnectionReadable();
// // String ipAddress =
// connection.getSocket().getInetAddress().getHostAddress();

// // boolean hasMismatch = checkMismatch(nodes, ipAddress);
// // byte status;
// // String message;
// // if (!hasMismatch && connections.containsKey(nodes)) {

// // connections.remove(nodes);
// // System.out.println("Deregistered " + nodes + ". There are now ("
// // + connections.size() + ") connections.\n");
// // message = "De-registration request successful. The number of messaging
// nodes
// // currently "
// // + "constituting the overlay is (" + connections.size() + ").\n";
// // status = Protocol.SUCCESS;
// // } else {
// // message = "Unable to process de-registration request. Responding with a
// // failure. Mismatch and connection exists? "
// // + hasMismatch + connections.containsKey(nodes);
// // System.out.println(message);
// // status = Protocol.FAILURE;
// // }
// // RegisterResponse response = new RegisterResponse(status, message);
// // try {
// // connection.getTCPSenderThread().sendData(response.getBytes());
// // } catch (IOException | InterruptedException e) {
// // System.out.println(e.getMessage());
// // e.printStackTrace();
// // }
// }

// private boolean checkMismatch(String nodeDetails, String connectionIP) {
// if (!nodeDetails.split(":")[0].equals(connectionIP)
// && !connectionIP.equals("localhost")) {
// return true;
// } else {
// return false;
// }
// }

// private void sendChunkServers(FetchChunkServers request, TCPConnection
// connection) {
// /*
// * TODO: get all the chunkservers that have free space at least the size of
// the
// * file
// */
// List<String> fileChunkServers = new ArrayList<>();

// /*
// * TODO: get 3 free chunk servers
// * this is for testing only
// */
// List<Map.Entry<String, ChunkServerMetadata>> sortedServers = new
// ArrayList<>(chunkServersMetadata.entrySet());
// Collections.sort(sortedServers,
// (a, b) -> Long.compare(b.getValue().getFreeSpace(),
// a.getValue().getFreeSpace()));
// List<Map.Entry<String, ChunkServerMetadata>> topThreeServers =
// sortedServers.subList(0,
// Math.min(3, sortedServers.size()));

// for (Map.Entry<String, ChunkServerMetadata> entry : topThreeServers) {
// fileChunkServers.add(entry.getKey());
// }

// try {
// ChunkServerList message = new ChunkServerList(fileChunkServers,
// request.getDestinationPath(), request.getSequence());
// connection.getTCPSenderThread().sendData(message.getBytes());

// } catch (Exception e) {
// System.out.println("Error sending chunk servers list: " + e.getMessage());
// e.printStackTrace();
// }

// }

// private void sendChunks(FetchChunksList message, TCPConnection connection) {
// try {
// /*
// * from the chunkAndServerMap, get all the chunks that contain the clusterPath
// * substring
// */
// List<String> keysContainingClusterPath = chunkAndServerMap.keySet().stream()
// .filter(key -> key.contains(message.clusterPath))
// .collect(Collectors.toList());

// List<String> validChunkServers = new ArrayList<>();

// for (String chunk : keysContainingClusterPath) {
// String targetServer = getValidChunkServer(chunkAndServerMap.get(chunk),
// message.clusterPath);
// validChunkServers.add(targetServer);
// }

// FetchChunksListResponse response = new
// FetchChunksListResponse(keysContainingClusterPath.size(),
// keysContainingClusterPath, validChunkServers, message.clusterPath,
// message.downloadPath);
// connection.getTCPSenderThread().sendData(response.getBytes());
// } catch (Exception e) {
// System.out.println("Error while sending fetch chunks list request: " +
// e.getMessage());
// e.printStackTrace();
// }
// }

// }
