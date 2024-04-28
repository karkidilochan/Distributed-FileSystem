package csx55.dfs.replication;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.Scanner;
import java.util.Timer;

import csx55.dfs.tcp.TCPConnection;
import csx55.dfs.tcp.TCPServer;
import csx55.dfs.utils.HeartBeat;
import csx55.dfs.utils.Node;
import csx55.dfs.wireformats.ChunkServerList;
import csx55.dfs.wireformats.ChunkMessage;
import csx55.dfs.wireformats.ChunkMessageResponse;
import csx55.dfs.wireformats.Event;
import csx55.dfs.wireformats.FetchChunkServers;
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
public class Client implements Node, Protocol {

    /*
     * port to listen for incoming connections, configured during messaging node
     * creation
     */
    private final Integer nodePort;
    private final String hostName;
    private final String hostIP;
    private final String fullAddress;

    // Constants for command strings

    // create a TCP connection with the Registry
    private TCPConnection controllerConnection;

    private Client(String hostName, String hostIP, int nodePort) {
        this.hostName = hostName;
        this.hostIP = hostIP;
        this.nodePort = nodePort;
        this.fullAddress = hostIP + ":" + nodePort;
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
            Client node = new Client(
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
        System.err.println("Usage: java csx55.chord.node.Client controller-ip controller-port");
        System.exit(1);
    }

    private void registerNode(String registryHost, Integer registryPort) {
        try {
            // create a socket to the Registry server
            Socket socketToRegistry = new Socket(registryHost, registryPort);
            TCPConnection connection = new TCPConnection(this, socketToRegistry);

            Register register = new Register(Protocol.CLIENT_REGISTER_REQUEST,
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

                    case "upload":
                        fetchChunkServers(input[1], input[2]);
                        break;

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

            case Protocol.CHUNK_SERVER_LIST:
                handleFileUpload((ChunkServerList) event);

            case Protocol.CHUNK_TRANSFER_RESPONSE:
                handleChunkTransferResponse((ChunkMessageResponse) event, connection);

        }
    }

    private void handleRegisterResponse(RegisterResponse response) {
        /* start the background routine to send heartbeat messages */
        Timer timerMajorHeartbeat = new Timer("MajorHeartbeat");
        /* this keeps delay 0 and period of 2 minutes = 120 * 1000 milliseconds */
        timerMajorHeartbeat.schedule(new HeartBeat(true, this.controllerConnection), 0, 120000);

        Timer timerMinorHeartbeat = new Timer("MinorHeartbeat");
        timerMinorHeartbeat.schedule(new HeartBeat(false, controllerConnection), 0, 15000);

        System.out.println("Received registration response from the discovery: " + response.toString());
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

    private void fetchChunkServers(String sourcePath, String destinationPath) {
        /*
         * my approach:
         * read file, get list of chunkservers in descending order of free space
         * controller will send list of chunkservers that have free space at least the
         * size of the file
         * after getting list of chunkservers, separate file into chunks
         * for each chunk pick a random chunkserver from the list and send the chunk to
         * it,
         * also pick two chunkservers and send it for replica
         */
        /*
         * split file into 64KB chunks
         * each chunk should keep checksum
         * checksum should be for 8KB slices of chunk -> done by chunk server ?
         */

        /*
         * first fetch a list of 3 chunk servers from controller
         * read file contents
         * split it into chunks
         * 
         */

        try {
            File file = new File(sourcePath);
            byte[] fileData = Files.readAllBytes(file.toPath());
            controllerConnection.getTCPSenderThread()
                    .sendData((new FetchChunkServers(sourcePath, destinationPath, fileData.length)).getBytes());

        } catch (IOException | InterruptedException e) {
            System.out.println("Error sending chunk servers fetch request: " + e.getMessage());
            e.printStackTrace();
        }

    }

    private void handleFileUpload(ChunkServerList message) {
        /*
         * after getting list of chunkservers, separate file into chunks
         * for each chunk pick a random chunkserver from the list and send the chunk to
         * it,
         * also pick two chunkservers and send it for replica
         */

        try {

            List<byte[]> chunks = getChunks(message.getSourcePath());
            List<String> chunkServers = message.getList();

            int sequenceNumber = 1;
            for (byte[] chunk : chunks) {
                String chunkServer = pickChunkServer(chunkServers);
                Socket socketToChunk = new Socket(message.getIPAddress(chunkServer), message.getPort(chunkServer));
                TCPConnection serverConnection = new TCPConnection(this, socketToChunk);

                /* pick any two chunkservers excluding this one */
                List<String> replicas = pickReplicas(chunkServer, chunkServers);

                ChunkMessage transfer = new ChunkMessage(message.getDestinationPath(), sequenceNumber, chunk,
                        replicas);

                serverConnection.getTCPSenderThread().sendData(transfer.getBytes());
                serverConnection.start();
            }

        } catch (Exception e) {
            System.out.println("Error sending chunks to chunkserver" + e.getMessage());
            e.printStackTrace();
        }

    }

    private List<byte[]> getChunks(String filePath) throws IOException {
        File file = new File(filePath);
        byte[] fileData = Files.readAllBytes(file.toPath());

        List<byte[]> chunks = new ArrayList<>();
        int offset = 0;
        int chunkSize = 64 * 1024; // 64KB
        int length;
        byte[] chunk;

        while (offset < fileData.length) {
            /* keeping length of bytes to read either chunksize or remaining bytes left */
            length = Math.min(chunkSize, fileData.length - offset);
            chunk = new byte[length];
            System.arraycopy(fileData, offset, chunk, 0, length);
            chunks.add(chunk);
            offset += length;
        }

        return chunks;

    }

    private String pickChunkServer(List<String> servers) {

        Random random = new Random();
        int randomIndex = random.nextInt(servers.size());
        String randomElement = servers.get(randomIndex);
        return randomElement;
    }

    private List<String> pickReplicas(String chunkServer, List<String> servers) {
        List<String> chunkServersCopy = new ArrayList<>(servers);
        List<String> replicas = new ArrayList<>();

        chunkServersCopy.remove(chunkServer);

        Random random = new Random();

        int randomIndex1 = random.nextInt(chunkServersCopy.size());
        int randomIndex2;

        do {
            randomIndex2 = random.nextInt(chunkServersCopy.size());
        } while (randomIndex2 == randomIndex1);

        String randomElement1 = chunkServersCopy.get(randomIndex1);
        String randomElement2 = chunkServersCopy.get(randomIndex2);

        replicas.add(randomElement1);
        replicas.add(randomElement2);

        return replicas;

    }

    private void handleChunkTransferResponse(ChunkMessageResponse message, TCPConnection connection) {
        System.out.println("Received chunk transfer response from the chunk server: " + message.toString());
        try {
            connection.close();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }

}
