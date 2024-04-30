// package csx55.dfs.erasure;

// import java.io.ByteArrayOutputStream;
// import java.io.File;
// import java.io.FileOutputStream;
// import java.io.IOException;
// import java.net.InetAddress;
// import java.net.ServerSocket;
// import java.net.Socket;
// import java.nio.file.Files;
// import java.util.ArrayList;
// import java.util.Date;
// import java.util.List;
// import java.util.Scanner;
// import java.util.concurrent.ConcurrentHashMap;

// import csx55.dfs.tcp.TCPConnection;
// import csx55.dfs.tcp.TCPServer;
// import csx55.dfs.utils.Node;
// import csx55.dfs.wireformats.ChunkServerList;
// import csx55.dfs.wireformats.ChunkMessage;
// import csx55.dfs.wireformats.ChunkMessageResponse;
// import csx55.dfs.wireformats.Event;
// import csx55.dfs.wireformats.FetchChunkServers;
// import csx55.dfs.wireformats.FetchChunksList;
// import csx55.dfs.wireformats.FetchChunksListResponse;
// import csx55.dfs.wireformats.Protocol;
// import csx55.dfs.wireformats.Register;
// import csx55.dfs.wireformats.RegisterResponse;
// import csx55.dfs.wireformats.RequestChunk;
// import csx55.dfs.wireformats.RequestChunkResponse;

// /**
// * Implementation of the Node interface, represents a messaging node in the
// * network overlay system.
// * Messaging nodes facilitate communication between other nodes in the
// overlay.
// * This class handles registration with a registry, establishment of
// * connections,
// * message routing, and messageStatistics tracking.
// */
// public class Client implements Node, Protocol {

// /*
// * port to listen for incoming connections, configured during messaging node
// * creation
// */
// private final Integer nodePort;
// private final String hostName;
// private final String hostIP;
// private final String fullAddress;

// // Constants for command strings
// private ConcurrentHashMap<String, List<byte[]>> fileChunksMap;

// // create a TCP connection with the Registry
// private TCPConnection controllerConnection;

// private Client(String hostName, String hostIP, int nodePort) {
// this.hostName = hostName;
// this.hostIP = hostIP;
// this.nodePort = nodePort;
// this.fullAddress = hostIP + ":" + nodePort;
// }

// public static void main(String[] args) {
// if (args.length < 2) {
// printUsageAndExit();
// }
// System.out.println("Chunk server is live at: " + new Date());
// try (ServerSocket serverSocket = new ServerSocket(0)) {
// // assign a random available port
// int nodePort = serverSocket.getLocalPort();

// String hostIP = InetAddress.getLocalHost().getHostAddress();

// /*
// * get local host name and use assigned nodePort to initialize a messaging
// node
// */
// Client node = new Client(
// InetAddress.getLocalHost().getHostName(), hostIP, nodePort);

// /* start a new TCP server thread */
// (new Thread(new TCPServer(node, serverSocket))).start();

// // register this node with the registry
// node.registerNode(args[0], Integer.valueOf(args[1]));

// // facilitate user input in the console
// node.takeCommands();
// } catch (IOException e) {
// System.out.println("An error occurred: " + e.getMessage());
// e.printStackTrace();
// }
// }

// /**
// * Print the correct usage of the program and exits with a non-zero status
// * code.
// */
// private static void printUsageAndExit() {
// System.err.println("Usage: java csx55.chord.node.Client controller-ip
// controller-port");
// System.exit(1);
// }

// private void registerNode(String registryHost, Integer registryPort) {
// try {
// // create a socket to the Registry server
// Socket socketToRegistry = new Socket(registryHost, registryPort);
// TCPConnection connection = new TCPConnection(this, socketToRegistry);

// Register register = new Register(Protocol.CLIENT_REGISTER_REQUEST,
// this.hostIP, this.nodePort, this.hostName);

// System.out.println(
// "Address of the chunk server node is: " + this.hostIP + ":" + this.nodePort);

// // send "Register" message to the Registry
// connection.getTCPSenderThread().sendData(register.getBytes());
// connection.start();

// // Set the registry connection for this node
// this.controllerConnection = connection;

// } catch (IOException | InterruptedException e) {
// System.out.println("Error registering node: " + e.getMessage());
// e.printStackTrace();
// }
// }

// private void takeCommands() {
// System.out.println(
// "Enter a command. Available commands: print-shortest-path, exit-overlay\n");
// try (Scanner scan = new Scanner(System.in)) {
// while (true) {
// String line = scan.nextLine().toLowerCase();
// String[] input = line.split("\\s+");
// switch (input[0]) {

// case "upload":
// fetchChunkServers(input[1], input[2]);
// break;

// case "download":
// fetchChunksList(input[1], input[2]);

// case "exit":
// // TODO:
// exitChord();
// break;

// default:
// System.out.println("Invalid Command. Available commands: exit\\n");
// break;
// }
// }
// } catch (Exception e) {
// System.err.println("An error occurred during command processing: " +
// e.getMessage());
// e.printStackTrace();
// } finally {
// System.out.println("De-registering the node and terminating: " + hostName +
// ":" + nodePort);
// exitChord();
// System.exit(0);
// }
// }

// public void handleIncomingEvent(Event event, TCPConnection connection) {
// // System.out.println("Received event: " + event.toString());

// switch (event.getType()) {

// case Protocol.REGISTER_RESPONSE:
// handleRegisterResponse((RegisterResponse) event);
// break;

// case Protocol.CHUNK_SERVER_LIST:
// handleFileUpload((ChunkServerList) event);

// case Protocol.CHUNK_TRANSFER_RESPONSE:
// handleChunkTransferResponse((ChunkMessageResponse) event, connection);

// case Protocol.FETCH_CHUNKS_RESPONSE:
// handleFetchChunksResponse((FetchChunksListResponse) event);

// case Protocol.REQUEST_CHUNK_RESPONSE:
// handleRequestChunkResponse((RequestChunkResponse) event, connection);

// }
// }

// private void handleRegisterResponse(RegisterResponse response) {
// System.out.println("Received registration response from the controller: " +
// response.toString());
// }

// private void exitChord() {
// /*
// * while exiting, send all the files you were responsible for to your
// successor
// */

// }

// public String getIPAddress() {
// return this.hostIP;
// }

// public int getPort() {
// return this.nodePort;
// }

// public String getFullAddress() {
// return fullAddress;
// }

// private void fetchChunkServers(String sourcePath, String destinationPath) {
// /*
// * read the file and create chunks
// * break it into k fragments => primary or data shards
// * encode and expand to n fragments
// * n > k
// * n = k + m
// * m => parity shards
// * then fetch chunk server for each of these fragments
// * k = 6, and m=3
// */

// try {

// List<byte[]> chunks = getChunks(sourcePath);
// fileChunksMap.put(destinationPath, chunks);

// for (int i = 1; i < chunks.size() + 1; i++) {
// controllerConnection.getTCPSenderThread()
// .sendData((new FetchChunkServers(destinationPath, i,
// chunks.size())).getBytes());
// /* TODO: keep a sleep here for this thread */
// }

// } catch (IOException | InterruptedException e) {
// System.out.println("Error sending chunk servers fetch request: " +
// e.getMessage());
// e.printStackTrace();
// }

// }

// private void handleFileUpload(ChunkServerList message) {
// /*
// * after getting list of chunkservers, separate file into chunks
// * for each chunk pick a random chunkserver from the list and send the chunk
// to
// * it,
// * also pick two chunkservers and send it for replica
// */

// try {
// List<byte[]> chunks = fileChunksMap.get(message.getDestinationPath());
// int sequenceNumber = message.getSequence();
// byte[] chunk = chunks.get(sequenceNumber - 1);

// List<String> chunkServers = message.getList();
// String chunkServer = chunkServers.get(0);
// Socket socketToChunk = new Socket(message.getIPAddress(chunkServer),
// message.getPort(chunkServer));
// TCPConnection serverConnection = new TCPConnection(this, socketToChunk);

// /* pick any two chunkservers excluding this one */

// List<String> replicas = new ArrayList<>();
// replicas.add(chunkServers.get(1));
// replicas.add(chunkServers.get(2));

// ChunkMessage transfer = new ChunkMessage(message.getDestinationPath(),
// sequenceNumber, chunk,
// replicas);

// serverConnection.getTCPSenderThread().sendData(transfer.getBytes());
// serverConnection.start();

// /*
// *
// * remove the file from files chunk map after all the chunk sequence have been
// * stored
// */
// if (sequenceNumber == chunks.size()) {
// fileChunksMap.remove(message.getDestinationPath());

// }

// } catch (Exception e) {
// System.out.println("Error sending chunks to chunkserver" + e.getMessage());
// e.printStackTrace();
// }

// }

// private List<byte[]> getChunks(String filePath) throws IOException {
// File file = new File(filePath);
// byte[] fileData = Files.readAllBytes(file.toPath());

// List<byte[]> chunks = new ArrayList<>();
// int offset = 0;
// int chunkSize = 64 * 1024; // 64KB
// int length;
// byte[] chunk;

// /* TODO: padding chunk with zeros if length is less than chunksize */

// while (offset < fileData.length) {
// /* keeping length of bytes to read either chunksize or remaining bytes left
// */
// length = Math.min(chunkSize, fileData.length - offset);
// chunk = new byte[length];
// System.arraycopy(fileData, offset, chunk, 0, length);
// chunks.add(chunk);
// offset += length;
// }

// return chunks;

// }

// private void handleChunkTransferResponse(ChunkMessageResponse message,
// TCPConnection connection) {
// System.out.println("Received chunk transfer response from the chunk server: "
// + message.toString());
// try {
// connection.close();

// } catch (Exception e) {
// System.out.println(e.getMessage());
// e.printStackTrace();
// }
// }

// private void fetchChunksList(String clusterPath, String downloadPath) {
// /* fetch list of the chunks of this file from cluster */
// try {
// FetchChunksList message = new FetchChunksList(clusterPath, downloadPath);
// controllerConnection.getTCPSenderThread().sendData(message.getBytes());
// } catch (IOException | InterruptedException e) {
// System.out.println("Error while sending fetch chunks list request: " +
// e.getMessage());
// e.printStackTrace();
// }

// }

// private void handleFetchChunksResponse(FetchChunksListResponse message) {
// for (int i = 0; i < message.numberOfChunks; i++) {
// try {
// RequestChunk request = new RequestChunk(message.downloadPath,
// message.chunksList.get(i), i + 1, message.numberOfChunks);

// String chunkServer = message.chunkServerList.get(i);

// Socket socket = new Socket(chunkServer.split(":")[0],
// Integer.valueOf(chunkServer.split(":")[1]));
// TCPConnection connection = new TCPConnection(this, socket);

// // send "Register" message to the Registry
// connection.getTCPSenderThread().sendData(request.getBytes());
// connection.start();
// } catch (IOException | InterruptedException e) {
// System.out.println("Error while sending request chunk: " + e.getMessage());
// e.printStackTrace();
// }

// }
// }

// private void handleRequestChunkResponse(RequestChunkResponse message,
// TCPConnection connection) {
// try {
// fileChunksMap.computeIfAbsent(message.getFilePath(), k -> new
// ArrayList<>(message.getTotalSize()));
// fileChunksMap.get(message.getFilePath()).add(message.getSequence() - 1,
// message.getChunk());

// connection.close();

// if (fileChunksMap.get(message.getFilePath()).size() ==
// message.getTotalSize()) {
// writeFile(message.getFilePath(), fileChunksMap.get(message.getFilePath()));
// }

// } catch (Exception e) {
// System.out.println(e.getMessage());
// e.printStackTrace();
// }
// }

// private void writeFile(String downloadPath, List<byte[]> chunksList) {
// try {
// ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

// for (byte[] chunk : chunksList) {
// byteArrayOutputStream.write(chunk);
// }

// FileOutputStream fileOutputStream = new FileOutputStream(downloadPath);
// fileOutputStream.write(byteArrayOutputStream.toByteArray());
// fileOutputStream.close();
// } catch (Exception e) {
// System.out.println("Error while writing file from chunks: " +
// e.getMessage());
// e.printStackTrace();
// }

// }

// // private String pickChunkServer(List<String> servers) {

// // Random random = new Random();
// // int randomIndex = random.nextInt(servers.size());
// // String randomElement = servers.get(randomIndex);
// // return randomElement;
// // }

// // private List<String> pickReplicas(String chunkServer, List<String>
// servers) {
// // List<String> chunkServersCopy = new ArrayList<>(servers);
// // List<String> replicas = new ArrayList<>();

// // chunkServersCopy.remove(chunkServer);

// // Random random = new Random();

// // int randomIndex1 = random.nextInt(chunkServersCopy.size());
// // int randomIndex2;

// // do {
// // randomIndex2 = random.nextInt(chunkServersCopy.size());
// // } while (randomIndex2 == randomIndex1);

// // String randomElement1 = chunkServersCopy.get(randomIndex1);
// // String randomElement2 = chunkServersCopy.get(randomIndex2);

// // replicas.add(randomElement1);
// // replicas.add(randomElement2);

// // return replicas;

// // }

// }
