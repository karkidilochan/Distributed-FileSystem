// package csx55.dfs.erasure;

// import java.io.IOException;
// import java.net.InetAddress;
// import java.net.ServerSocket;
// import java.net.Socket;
// import java.util.Date;
// import java.util.Map;
// import java.util.Scanner;

// import csx55.dfs.tcp.TCPConnection;
// import csx55.dfs.tcp.TCPServer;
// import csx55.dfs.utils.Node;
// import csx55.dfs.wireformats.Event;
// import csx55.dfs.wireformats.FileTransfer;
// import csx55.dfs.wireformats.FileTransferResponse;
// import csx55.dfs.wireformats.Protocol;
// import csx55.dfs.wireformats.Register;
// import csx55.dfs.wireformats.RegisterResponse;

// /**
//  * Implementation of the Node interface, represents a messaging node in the
//  * network overlay system.
//  * Messaging nodes facilitate communication between other nodes in the overlay.
//  * This class handles registration with a registry, establishment of
//  * connections,
//  * message routing, and messageStatistics tracking.
//  */
// public class Client implements Node, Protocol {

//     /*
//      * port to listen for incoming connections, configured during messaging node
//      * creation
//      */
//     private final Integer nodePort;
//     private final String hostName;
//     private Integer peerID;
//     private final String hostIP;
//     private final String fullAddress;

//     // Constants for command strings

//     // create a TCP connection with the Registry
//     private TCPConnection registryConnection;

//     private Client(String hostName, String hostIP, int nodePort, int peerID) {
//         this.hostName = hostName;
//         this.hostIP = hostIP;
//         this.nodePort = nodePort;
//         this.peerID = peerID;
//         this.fullAddress = hostIP + ":" + nodePort;
//     }

//     public static void main(String[] args) {
//         if (args.length < 2) {
//             printUsageAndExit();
//         }
//         System.out.println("Client is live at: " + new Date());
//         try (ServerSocket serverSocket = new ServerSocket(0)) {
//             // assign a random available port
//             int nodePort = serverSocket.getLocalPort();

//             String hostIP = InetAddress.getLocalHost().getHostAddress();

//             /* 32-bit integer id for client */
//             String hostString = hostIP + ":" + String.valueOf(nodePort);
//             int peerID = Math.abs(hostString.hashCode());
//             System.out.println("hashcode of " + hostString + " " + peerID);

//             /*
//              * get local host name and use assigned nodePort to initialize a messaging node
//              */
//             Client node = new Client(
//                     InetAddress.getLocalHost().getHostName(), hostIP, nodePort, peerID);

//             /* start a new TCP server thread */
//             (new Thread(new TCPServer(node, serverSocket))).start();

//             // register this node with the registry
//             node.registerNode(args[0], Integer.valueOf(args[1]));

//             // facilitate user input in the console
//             node.takeCommands();
//         } catch (IOException e) {
//             System.out.println("An error occurred: " + e.getMessage());
//             e.printStackTrace();
//         }
//     }

//     /**
//      * Print the correct usage of the program and exits with a non-zero status
//      * code.
//      */
//     private static void printUsageAndExit() {
//         System.err.println("Usage: java csx55.chord.node.ChunkServer controller-ip controller-port");
//         System.exit(1);
//     }

//     private void registerNode(String registryHost, Integer registryPort) {
//         try {
//             // create a socket to the Registry server
//             Socket socketToRegistry = new Socket(registryHost, registryPort);
//             TCPConnection connection = new TCPConnection(this, socketToRegistry);

//             Register register = new Register(Protocol.REGISTER_REQUEST,
//                     this.hostIP, this.nodePort, this.hostName, this.peerID);

//             System.out.println(
//                     "Address of the client node is: " + this.hostIP + ":" + this.nodePort);

//             // send "Register" message to the Registry
//             connection.getTCPSenderThread().sendData(register.getBytes());
//             connection.start();

//             // Set the registry connection for this node
//             this.registryConnection = connection;

//         } catch (IOException | InterruptedException e) {
//             System.out.println("Error registering node: " + e.getMessage());
//             e.printStackTrace();
//         }
//     }

//     private void takeCommands() {
//         System.out.println(
//                 "Enter a command. Available commands: print-shortest-path, exit-overlay\n");
//         try (Scanner scan = new Scanner(System.in)) {
//             while (true) {
//                 String line = scan.nextLine().toLowerCase();
//                 String[] input = line.split("\\s+");
//                 switch (input[0]) {

//                     case "exit":
//                         // TODO:
//                         exitChord();
//                         break;

//                     default:
//                         System.out.println("Invalid Command. Available commands: exit\\n");
//                         break;
//                 }
//             }
//         } catch (Exception e) {
//             System.err.println("An error occurred during command processing: " + e.getMessage());
//             e.printStackTrace();
//         } finally {
//             System.out.println("Deregistering the node and terminating: " + hostName + ":" + nodePort);
//             exitChord();
//             System.exit(0);
//         }
//     }

//     public void handleIncomingEvent(Event event, TCPConnection connection) {
//         // System.out.println("Received event: " + event.toString());

//         switch (event.getType()) {

//             case Protocol.REGISTER_RESPONSE:
//                 handleRegisterResponse((RegisterResponse) event);
//                 break;

//         }
//     }

//     private void retryRegistration() {
//         try {
//             // create a socket to the Registry server
//             this.peerID = Math.abs(this.fullAddress.hashCode());

//             Register register = new Register(Protocol.REGISTER_REQUEST,
//                     this.hostIP, this.nodePort, this.hostName, this.peerID);

//             // send "Register" message to the Registry
//             this.registryConnection.getTCPSenderThread().sendData(register.getBytes());

//             /* initialize finger table */

//         } catch (IOException | InterruptedException e) {
//             System.out.println("Error registering node: " + e.getMessage());
//             e.printStackTrace();
//         }
//     }

//     private void handleRegisterResponse(RegisterResponse response) {

//         System.out.println("Received registration response from the discovery: " + response.toString());
//     }

//     private void handleDiscoveryMessage(SetupChord message) {
//         /*
//          * first check if the setup chord is self or not i.e. its the first client in
//          * the
//          * chord
//          */
//         if (!message.getConnectionReadable().equals(fullAddress)) {
//             try {
//                 Socket socketToPeer = new Socket(message.getIPAddress(), message.getPort());
//                 TCPConnection connection = new TCPConnection(this, socketToPeer);

//                 /* send lookup request of the peer-id to this random client */

//                 /*
//                  * Basic workflow:
//                  * 1. send request successor to random node
//                  * 2. random node searches for your immediate predecessor, pings it through
//                  * forwarded requests i.e. perform lookup hops with requests
//                  * 3. the predecessor pings you with its successor
//                  * 4. you update your successor with this info
//                  */

//             } catch (IOException | InterruptedException e) {
//                 System.out.println(e.getMessage());
//                 e.printStackTrace();
//             }
//         }

//     }

//     private void exitChord() {
//         /*
//          * while exiting, send all the files you were responsible for to your successor
//          */

//     }

//     public String getIPAddress() {
//         return this.hostIP;
//     }

//     public int getPort() {
//         return this.nodePort;
//     }

//     public int getPeerID() {
//         return this.peerID;
//     }

//     public String getFullAddress() {
//         return fullAddress;
//     }

// }
