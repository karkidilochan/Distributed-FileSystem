package csx55.dfs.replication;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Scanner;
import java.util.Timer;

import csx55.dfs.tcp.TCPConnection;
import csx55.dfs.tcp.TCPServer;
import csx55.dfs.utils.HeartBeat;
import csx55.dfs.utils.Node;
import csx55.dfs.wireformats.ChunkMessage;
import csx55.dfs.wireformats.ChunkMessageResponse;
import csx55.dfs.wireformats.Event;
import csx55.dfs.wireformats.Protocol;
import csx55.dfs.wireformats.Register;
import csx55.dfs.wireformats.RegisterResponse;

/**
 * Implementation of the Node interface, represents a messaging node in the
 * network overlay system.
 * Messaging nodes facilitate communication between other nodes in the overlay.
 * This class handles registration with a registry, establishment of
 * connections,
 * message routing, and messageStatistics tracking.
 */
public class ChunkServer implements Node, Protocol {

    /*
     * port to listen for incoming connections, configured during messaging node
     * creation
     */
    private final Integer nodePort;
    private final String hostName;
    private final String hostIP;
    private final String fullAddress;
    private final List<Chunk> chunksList;
    private final List<Chunk> newChunksList;

    private final String chunkPathPrefix = "/tmp/chunk-server/";

    // Constants for command strings

    // create a TCP connection with the Registry
    private TCPConnection controllerConnection;

    private ChunkServer(String hostName, String hostIP, int nodePort) {
        this.hostName = hostName;
        this.hostIP = hostIP;
        this.nodePort = nodePort;
        this.fullAddress = hostIP + ":" + nodePort;
        this.chunksList = new ArrayList<>();
        this.newChunksList = new ArrayList<>();
    }

    public static void main(String[] args) {
        if (args.length < 2) {
            printUsageAndExit();
        }
        System.out.println("Chunk server is live at: " + new Date());
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            // assign a random available port
            int nodePort = serverSocket.getLocalPort();

            String hostIP = InetAddress.getLocalHost().getHostAddress();

            /*
             * get local host name and use assigned nodePort to initialize a messaging node
             */
            ChunkServer node = new ChunkServer(
                    InetAddress.getLocalHost().getHostName(), hostIP, nodePort);

            /* start a new TCP server thread */
            (new Thread(new TCPServer(node, serverSocket))).start();

            // register this node with the registry
            node.registerNode(args[0], Integer.valueOf(args[1]));

            // facilitate user input in the console
            node.takeCommands();
        } catch (IOException e) {
            System.out.println("An error occurred: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Print the correct usage of the program and exits with a non-zero status
     * code.
     */
    private static void printUsageAndExit() {
        System.err.println("Usage: java csx55.chord.node.ChunkServer controller-ip controller-port");
        System.exit(1);
    }

    private void registerNode(String registryHost, Integer registryPort) {
        try {
            // create a socket to the Registry server
            Socket socketToRegistry = new Socket(registryHost, registryPort);
            TCPConnection connection = new TCPConnection(this, socketToRegistry);

            Register register = new Register(Protocol.CHUNK_SERVER_REGISTER_REQUEST,
                    this.hostIP, this.nodePort, this.hostName);

            System.out.println(
                    "Address of the chunk server node is: " + this.hostIP + ":" + this.nodePort);

            // send "Register" message to the Registry
            connection.getTCPSenderThread().sendData(register.getBytes());
            connection.start();

            // Set the registry connection for this node
            this.controllerConnection = connection;

        } catch (IOException | InterruptedException e) {
            System.out.println("Error registering node: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void takeCommands() {
        System.out.println(
                "Enter a command. Available commands: print-shortest-path, exit-overlay\n");
        try (Scanner scan = new Scanner(System.in)) {
            while (true) {
                String line = scan.nextLine().toLowerCase();
                String[] input = line.split("\\s+");
                switch (input[0]) {

                    case "exit":
                        // TODO:
                        exitChord();
                        break;

                    default:
                        System.out.println("Invalid Command. Available commands: exit\\n");
                        break;
                }
            }
        } catch (Exception e) {
            System.err.println("An error occurred during command processing: " + e.getMessage());
            e.printStackTrace();
        } finally {
            System.out.println("De-registering the node and terminating: " + hostName + ":" + nodePort);
            exitChord();
            System.exit(0);
        }
    }

    public void handleIncomingEvent(Event event, TCPConnection connection) {
        // System.out.println("Received event: " + event.toString());

        switch (event.getType()) {

            case Protocol.REGISTER_RESPONSE:
                handleRegisterResponse((RegisterResponse) event);
                break;

            case Protocol.CHUNK_TRANSFER:
                handleChunkUpload((ChunkMessage) event, connection);

        }
    }

    private void handleRegisterResponse(RegisterResponse response) {
        /* TODO: prepare heartbeat payload and send heartbeat messages to controller */
        /* start the background routine to send heartbeat messages */
        // Timer timerMajorHeartbeat = new Timer("MajorHeartbeat");
        // /* this keeps delay 0 and period of 2 minutes = 120 * 1000 milliseconds */
        // int majorHeartbeatInterval = 120000;
        // timerMajorHeartbeat.schedule(new HeartBeat(true, this.controllerConnection),
        // 0, 15000);

        // Timer timerMinorHeartbeat = new Timer("MinorHeartbeat");
        // timerMinorHeartbeat.schedule(new HeartBeat(false, controllerConnection), 0,
        // 5000);

        System.out.println("Received registration response from the discovery: " + response.toString());
    }

    private void handleChunkUpload(ChunkMessage message, TCPConnection connection) {
        /*
         * get the chunk and store it along with metadata
         * forward the chunk to the first element of replicas list
         * send the second element of replicas list as payload, which the first one will
         * use to further create a replica
         */

        Chunk chunk = new Chunk(message.getSequence(), message.getFilePath());

        try {
            chunk.createChecksumSlices(message.getChunk());

            boolean isSuccessful = chunk.writeChunk(chunkPathPrefix, message);

            byte status;
            String response;
            if (isSuccessful) {
                status = Protocol.SUCCESS;
                response = "Chunk creation was successful.";

                chunksList.add(chunk);
                newChunksList.add(chunk);
            } else {
                status = Protocol.FAILURE;
                response = "Chunk creation failed.";
            }

            ChunkMessageResponse request = new ChunkMessageResponse(status, response);
            connection.getTCPSenderThread().sendData(request.getBytes());

        } catch (Exception e) {
            System.out.println("Error while handling chunk upload" + e.getMessage());
            e.printStackTrace();
        }

        /*
         * first create the filename of the chunk with sequence number like this
         * /SimFile.data_chunk2
         * write it to the chunk directory
         * keep track of it in the chunks list and new chunks list for the heartbeat
         * then, create 8KB slices of the chunk and generate sha-1 checksum for each
         * add those hashes into the list of the chunk object
         */

    }

    private void exitChord() {
        /*
         * while exiting, send all the files you were responsible for to your successor
         */

    }

    public String getIPAddress() {
        return this.hostIP;
    }

    public int getPort() {
        return this.nodePort;
    }

    public String getFullAddress() {
        return fullAddress;
    }

}
