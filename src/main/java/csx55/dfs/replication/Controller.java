package csx55.dfs.replication;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import csx55.dfs.tcp.TCPConnection;
import csx55.dfs.tcp.TCPServer;
import csx55.dfs.utils.Node;
import csx55.dfs.wireformats.Event;
import csx55.dfs.wireformats.FetchChunkServers;
import csx55.dfs.wireformats.Protocol;
import csx55.dfs.wireformats.Register;
import csx55.dfs.wireformats.RegisterResponse;
import csx55.dfs.wireformats.ChunkServerList;
import csx55.dfs.wireformats.MajorHeartbeat;
import csx55.dfs.wireformats.MinorHeartbeat;

public class Controller implements Node {
    // Constants representing different commands

    private Map<String, TCPConnection> clientConnections = new HashMap<>();

    private Map<String, TCPConnection> chunkServerConnections = new HashMap<>();

    public static void main(String[] args) {
        // Check if the port number is provided as a command-line argument
        if (args.length < 1) {
            System.out.println("Error starting the Registry. Usage: java csx55.overlay.node.Registry portnum");
        }

        Controller registry = new Controller();

        /*
         * defining serverSocket in try-with-resources statement ensures
         * that the serverSocket is closed after the block ends
         */
        try (ServerSocket serverSocket = new ServerSocket(Integer.valueOf(args[0]))) {
            /*
             * start the server thread after initializing the server socket
             * invoke start function to start a new thread execution(invoking run() is not
             * the right way)
             */
            (new Thread(new TCPServer(registry, serverSocket))).start();

            // Take commands from console
            registry.takeCommands();
        } catch (IOException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }

    /* Takes user commands from console */
    private void takeCommands() {
        System.out
                .println("This is the Controller command console. Please enter a valid command to start the overlay.");

        try (Scanner scan = new Scanner(System.in)) {
            while (true) {
                String line = scan.nextLine().toLowerCase();
                String[] input = line.split("\\s+");
                switch (input[0]) {

                    default:
                        System.out.println("Please enter a valid command! Options are:\n" +
                                " - peer-nodes\n");
                        break;
                }
            }
        }
    }

    public void handleIncomingEvent(Event event, TCPConnection connection) {
        switch (event.getType()) {
            case Protocol.CLIENT_REGISTER_REQUEST:
                handleClientRegistration((Register) event, connection);
                break;

            case Protocol.CHUNK_SERVER_REGISTER_REQUEST:
                handleChunkServerRegistration((Register) event, connection);
                break;

            case Protocol.DEREGISTER_REQUEST:
                handleDeregistrationEvent((Register) event, connection);
                break;

            case Protocol.MAJOR_HEARTBEAT:
                handleMajorHeartbeat((MajorHeartbeat) event);
                break;

            case Protocol.MINOR_HEARTBEAT:
                handleMinorHeartbeat((MinorHeartbeat) event);
                break;

            case Protocol.FETCH_CHUNK_SERVERS:
                sendChunkServers((FetchChunkServers) event, connection);
        }
    }

    private synchronized void handleClientRegistration(Register registerEvent, TCPConnection connection) {
        // typecast event object to Register
        String nodes = registerEvent.getConnectionReadable();
        String ipAddress = connection.getSocket().getInetAddress().getHostAddress();

        boolean hasMismatch = checkMismatch(nodes, ipAddress);
        byte status;
        String message;

        /* TODO: detect identifier collision */
        if (!hasMismatch) {
            /* validate peer id */

            clientConnections.put(nodes, connection);

            message = "Registration request successful.  The number of clients currently is ("
                    + clientConnections.size() + ").\n";
            status = Protocol.SUCCESS;
            System.out.println("Connected Node: " + nodes);

        } else {
            message = "Unable to process request. Responding with a failure while mismatch is" + hasMismatch;
            System.out.println(message);
            status = Protocol.FAILURE;
        }
        RegisterResponse response = new RegisterResponse(status, message);
        try {
            connection.getTCPSenderThread().sendData(response.getBytes());
        } catch (IOException | InterruptedException e) {
            System.out.println(e.getMessage());
            clientConnections.remove(nodes);
            e.printStackTrace();
        }

    }

    private synchronized void handleChunkServerRegistration(Register registerEvent, TCPConnection connection) {
        // typecast event object to Register
        String nodes = registerEvent.getConnectionReadable();
        String ipAddress = connection.getSocket().getInetAddress().getHostAddress();

        boolean hasMismatch = checkMismatch(nodes, ipAddress);
        byte status;
        String message;

        /* TODO: detect identifier collision */
        if (!hasMismatch) {
            /* validate peer id */

            chunkServerConnections.put(nodes, connection);

            message = "Registration request successful.  The number of chunk servers currently is ("
                    + chunkServerConnections.size() + ").\n";
            status = Protocol.SUCCESS;
            System.out.println("Connected Node: " + nodes);

        } else {
            message = "Unable to process request. Responding with a failure while mismatch is" + hasMismatch;
            System.out.println(message);
            status = Protocol.FAILURE;
        }
        RegisterResponse response = new RegisterResponse(status, message);
        try {
            connection.getTCPSenderThread().sendData(response.getBytes());
        } catch (IOException | InterruptedException e) {
            System.out.println(e.getMessage());
            chunkServerConnections.remove(nodes);
            e.printStackTrace();
        }

    }

    /* TODO: handle exit of chunkserver and client */
    private synchronized void handleDeregistrationEvent(Register registerEvent, TCPConnection connection) {
        // // typecast event object to Register
        // String nodes = registerEvent.getConnectionReadable();
        // String ipAddress = connection.getSocket().getInetAddress().getHostAddress();

        // boolean hasMismatch = checkMismatch(nodes, ipAddress);
        // byte status;
        // String message;
        // if (!hasMismatch && connections.containsKey(nodes)) {

        // connections.remove(nodes);
        // System.out.println("Deregistered " + nodes + ". There are now ("
        // + connections.size() + ") connections.\n");
        // message = "De-registration request successful. The number of messaging nodes
        // currently "
        // + "constituting the overlay is (" + connections.size() + ").\n";
        // status = Protocol.SUCCESS;
        // } else {
        // message = "Unable to process de-registration request. Responding with a
        // failure. Mismatch and connection exists? "
        // + hasMismatch + connections.containsKey(nodes);
        // System.out.println(message);
        // status = Protocol.FAILURE;
        // }
        // RegisterResponse response = new RegisterResponse(status, message);
        // try {
        // connection.getTCPSenderThread().sendData(response.getBytes());
        // } catch (IOException | InterruptedException e) {
        // System.out.println(e.getMessage());
        // e.printStackTrace();
        // }
    }

    private boolean checkMismatch(String nodeDetails, String connectionIP) {
        if (!nodeDetails.split(":")[0].equals(connectionIP)
                && !connectionIP.equals("localhost")) {
            return true;
        } else {
            return false;
        }
    }

    private void handleMajorHeartbeat(MajorHeartbeat message) {
        System.out.println("received major heartbeat");
        // send metadata of all chunks
        // plus, total no of chunks, free space available(1GB - space used so far)
    }

    private void handleMinorHeartbeat(MinorHeartbeat message) {
        System.out.println("received minor heartbeat");
        // send metadata info of newly added chunks
        // report file corruption if detected in this heart beat
    }

    private void sendChunkServers(FetchChunkServers request, TCPConnection connection) {
        /*
         * TODO: get all the chunkservers that have free space at least the size of the
         * file
         */
        String chunkServerA = "";
        for (Map.Entry<String, TCPConnection> entry : chunkServerConnections.entrySet()) {
            chunkServerA = entry.getKey();
        }
        try {
            ChunkServerList message = new ChunkServerList(chunkServerA, chunkServerA, chunkServerA,
                    request.getSourcePath(),
                    request.getDestinationPath());
            connection.getTCPSenderThread().sendData(message.getBytes());

        } catch (Exception e) {
            System.out.println("Error sending chunk servers list: " + e.getMessage());
            e.printStackTrace();
        }

    }

}
