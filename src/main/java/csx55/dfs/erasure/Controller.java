// package csx55.dfs.erasure;

// import java.io.IOException;
// import java.net.ServerSocket;
// import java.util.ArrayList;
// import java.util.HashMap;
// import java.util.List;
// import java.util.Map;
// import java.util.Random;
// import java.util.Scanner;

// import csx55.dfs.tcp.TCPConnection;
// import csx55.dfs.tcp.TCPServer;
// import csx55.dfs.wireformats.Event;
// import csx55.dfs.wireformats.Protocol;
// import csx55.dfs.wireformats.Register;
// import csx55.dfs.wireformats.RegisterResponse;
// import csx55.dfs.wireformats.SetupChord;

// public class Controller implements Node {
//     // Constants representing different commands
//     private static final String PEER_NODES = "peer-nodes";

//     private Map<String, TCPConnection> connections = new HashMap<>();

//     public static void main(String[] args) {
//         // Check if the port number is provided as a command-line argument
//         if (args.length < 1) {
//             System.out.println("Error starting the Registry. Usage: java csx55.overlay.node.Registry portnum");
//         }

//         Controller registry = new Controller();

//         /*
//          * defining serverSocket in try-with-resources statement ensures
//          * that the serverSocket is closed after the block ends
//          */
//         try (ServerSocket serverSocket = new ServerSocket(Integer.valueOf(args[0]))) {
//             /*
//              * start the server thread after initializing the server socket
//              * invoke start function to start a new thread execution(invoking run() is not
//              * the right way)
//              */
//             (new Thread(new TCPServer(registry, serverSocket))).start();

//             // Take commands from console
//             registry.takeCommands();
//         } catch (IOException e) {
//             System.out.println(e.getMessage());
//             e.printStackTrace();
//         }
//     }

//     /* Takes user commands from console */
//     private void takeCommands() {
//         System.out.println("This is the Registry command console. Please enter a valid command to start the overlay.");

//         try (Scanner scan = new Scanner(System.in)) {
//             while (true) {
//                 String line = scan.nextLine().toLowerCase();
//                 String[] input = line.split("\\s+");
//                 switch (input[0]) {
//                     case PEER_NODES:
//                         listPeerNodes();
//                         break;

//                     default:
//                         System.out.println("Please enter a valid command! Options are:\n" +
//                                 " - peer-nodes\n");
//                         break;
//                 }
//             }
//         }
//     }

//     public void handleIncomingEvent(Event event, TCPConnection connection) {
//         switch (event.getType()) {
//             case Protocol.REGISTER_REQUEST:
//                 handleRegistrationEvent((Register) event, connection);
//                 break;

//             case Protocol.DEREGISTER_REQUEST:
//                 handleDeregistrationEvent((Register) event, connection);
//                 break;
//         }
//     }

//     private synchronized void handleRegistrationEvent(Register registerEvent, TCPConnection connection) {
//         // typecast event object to Register
//         String nodes = registerEvent.getConnectionReadable();
//         String ipAddress = connection.getSocket().getInetAddress().getHostAddress();

//         if (connections.containsKey(nodes)) {
//             try {
//                 connection.getTCPSenderThread().sendData((new Collision()).getBytes());
//                 return;

//             } catch (IOException | InterruptedException e) {
//                 System.out.println(e.getMessage());
//                 connections.remove(nodes);
//                 e.printStackTrace();
//                 return;
//             }
//         }

//         boolean hasMismatch = checkMismatch(nodes, ipAddress);
//         byte status;
//         String message;

//         String randomKey = null;

//         /* TODO: detect identifier collision */
//         if (!hasMismatch && validatePeerID(registerEvent)) {
//             /* validate peer id */

//             if (connections.size() == 0) {
//                 randomKey = nodes;
//             } else {
//                 Random random = new Random();
//                 int randomIndex = random.nextInt(connections.keySet().size());

//                 // Retrieve the key at the random index
//                 List<String> keysList = new ArrayList<>(connections.keySet());
//                 randomKey = keysList.get(randomIndex);
//             }

//             connections.put(nodes, connection);

//             message = "Registration request successful.  The number of messaging nodes currently "
//                     + "constituting the overlay is (" + connections.size() + ").\n";
//             status = Protocol.SUCCESS;
//             System.out.println("Connected Node: " + nodes + " Peer ID: " + registerEvent.getPeerID());

//         } else {
//             message = "Unable to process request. Responding with a failure while peerID validation is "
//                     + validatePeerID(registerEvent) + " and ping mismatch " + hasMismatch;
//             System.out.println(message);
//             status = Protocol.FAILURE;
//         }
//         RegisterResponse response = new RegisterResponse(status, message);
//         try {
//             connection.getTCPSenderThread().sendData(response.getBytes());
//         } catch (IOException | InterruptedException e) {
//             System.out.println(e.getMessage());
//             connections.remove(nodes);
//             e.printStackTrace();
//         }

//         /* send live peer info after sending register response message */

//         if (status == Protocol.SUCCESS) {
//             sendLivePeerInfo(connection, randomKey);
//         }
//     }

//     private synchronized void handleDeregistrationEvent(Register registerEvent, TCPConnection connection) {
//         // typecast event object to Register
//         String nodes = registerEvent.getConnectionReadable();
//         String ipAddress = connection.getSocket().getInetAddress().getHostAddress();

//         boolean hasMismatch = checkMismatch(nodes, ipAddress);
//         byte status;
//         String message;
//         if (!hasMismatch && connections.containsKey(nodes)) {

//             connections.remove(nodes);
//             System.out.println("Deregistered " + nodes + ". There are now ("
//                     + connections.size() + ") connections.\n");
//             message = "Deregistration request successful.  The number of messaging nodes currently "
//                     + "constituting the overlay is (" + connections.size() + ").\n";
//             status = Protocol.SUCCESS;
//         } else {
//             message = "Unable to process deregistration request. Responding with a failure. Mismatch and connection exists? "
//                     + hasMismatch + connections.containsKey(nodes);
//             System.out.println(message);
//             status = Protocol.FAILURE;
//         }
//         RegisterResponse response = new RegisterResponse(status, message);
//         try {
//             connection.getTCPSenderThread().sendData(response.getBytes());
//         } catch (IOException | InterruptedException e) {
//             System.out.println(e.getMessage());
//             e.printStackTrace();
//         }
//     }

//     private boolean checkMismatch(String nodeDetails, String connectionIP) {
//         if (!nodeDetails.split(":")[0].equals(connectionIP)
//                 && !connectionIP.equals("localhost")) {
//             return true;
//         } else {
//             return false;
//         }
//     }

//     private boolean validatePeerID(Register registerEvent) {
//         return Math.abs(registerEvent.getConnectionReadable().hashCode()) == registerEvent.getPeerID();

//     }

//     private void listPeerNodes() {
//         if (connections.size() == 0) {
//             System.out.println(
//                     "No connections in the registry.");
//         } else {
//             connections.forEach((key, value) -> System.out.println(Math.abs(key.hashCode()) + " " + key));
//         }
//     }

//     private void sendLivePeerInfo(TCPConnection connection, String randomKey) {
//         SetupChord message;

//         // if (connections.size() == 1) {
//         // message = new SetupChord(true);
//         // } else {
//         // /* send a random live peers network information */

//         String[] parts = randomKey.split(":");

//         message = new SetupChord(parts[0], Integer.parseInt(parts[1]));
//         try {
//             connection.getTCPSenderThread().sendData(message.getBytes());
//         } catch (IOException | InterruptedException e) {
//             System.out.println(e.getMessage());
//             e.printStackTrace();
//         }
//         // }

//     }

// }
