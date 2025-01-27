package distributed.dfs.replication;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
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
import java.util.Map;
import java.util.Scanner;
import java.util.Timer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import distributed.dfs.tcp.TCPConnection;
import distributed.dfs.tcp.TCPServer;
import distributed.dfs.utils.HeartBeat;
import distributed.dfs.utils.Node;
import distributed.dfs.wireformats.ChunkCorrection;
import distributed.dfs.wireformats.ChunkMessage;
import distributed.dfs.wireformats.ChunkMessageResponse;
import distributed.dfs.wireformats.CreateReplica;
import distributed.dfs.wireformats.CreateReplicaResponse;
import distributed.dfs.wireformats.ErrorCorrection;
import distributed.dfs.wireformats.Event;
import distributed.dfs.wireformats.MigrateChunk;
import distributed.dfs.wireformats.MigrationResponse;
import distributed.dfs.wireformats.Protocol;
import distributed.dfs.wireformats.Register;
import distributed.dfs.wireformats.RegisterResponse;
import distributed.dfs.wireformats.ReplicateNewServer;
import distributed.dfs.wireformats.ReportChunkCorruption;
import distributed.dfs.wireformats.RequestChunk;
import distributed.dfs.wireformats.RequestChunkResponse;

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
    // private final List<Chunk> chunksList;

    /* this will be the map of fileName and the corresponding chunk */
    private final ConcurrentHashMap<String, List<Chunk>> fileChunksMap = new ConcurrentHashMap<>();
    // private final List<String> chunksPathList = new ArrayList<>();
    private final List<String> newChunksList = new CopyOnWriteArrayList<String>();
    private final List<String> allChunksList = new ArrayList<>();

    private final String chunkPathPrefix = "/tmp/chunk-server/";
    private volatile long freeSpace = 1024 * 1024 * 1024; // 1GB
    private volatile long numberOfChunks = 0;

    Timer timerMajorHeartbeat = new Timer("MajorHeartbeat");
    Timer timerMinorHeartbeat = new Timer("MinorHeartbeat");

    // Constants for command strings

    // create a TCP connection with the Registry
    private TCPConnection controllerConnection;

    private ChunkServer(String hostName, String hostIP, int nodePort) {
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
        System.err.println("Usage: java distributed.chord.node.ChunkServer controller-ip controller-port");
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
                        exitDfs();
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
            exitDfs();
            System.exit(0);
        }
    }

    public void handleIncomingEvent(Event event, TCPConnection connection) {
        System.out.println("Received event: " + event.toString());

        switch (event.getType()) {

            case Protocol.REGISTER_RESPONSE:
                handleRegisterResponse((RegisterResponse) event);
                break;

            case Protocol.CHUNK_TRANSFER:
                handleChunkUpload((ChunkMessage) event, connection);
                break;

            case Protocol.CREATE_REPLICA:
                createReplica((CreateReplica) event, connection);
                break;

            case Protocol.CREATE_REPLICA_RESPONSE:
                handleCreateReplicaResponse((CreateReplicaResponse) event, connection);
                break;

            case Protocol.REQUEST_CHUNK:
                handleRequestChunk((RequestChunk) event, connection);
                break;

            case Protocol.ERROR_CORRECTION:
                handleErrorCorrection((ErrorCorrection) event);
                break;

            case Protocol.CHUNK_CORRECTION:
                handleChunkCorrection((ChunkCorrection) event);
                break;

            case Protocol.REPLICATE_NEW_SERVER:
                handleReplication((ReplicateNewServer) event);
                break;

            case Protocol.MIGRATE_CHUNK:
                handleChunkMigration((MigrateChunk) event, connection);
                break;

            case Protocol.MIGRATE_CHUNK_RESPONSE:
                handleMigrationResponse((MigrationResponse) event);
                break;

        }
    }

    private void handleRegisterResponse(RegisterResponse response) {
        /* TODO: prepare heartbeat payload and send heartbeat messages to controller */
        /* start the background routine to send heartbeat messages */
        /* this keeps delay 0 and period of 2 minutes = 2 * 60 * 1000 milliseconds */
        // int majorHeartbeatInterval = 120000;

        timerMajorHeartbeat.schedule(new HeartBeat(true, this.controllerConnection, this),
                0, 120000);

        /* minor heartbeat interval should be 15 seconds */
        timerMinorHeartbeat.schedule(new HeartBeat(false, controllerConnection, this), 0,
                15000);

        System.out.println("Received registration response from the discovery: " + response.toString());
    }

    private void handleChunkUpload(ChunkMessage message, TCPConnection connection) {
        /*
         * get the chunk and store it along with metadata
         * forward the chunk to the first element of replicas list
         * send the second element of replicas list as payload, which the first one will
         * use to further create a replica
         */

        Chunk chunk = new Chunk(message.getSequence());

        try {
            chunk.createChecksumSlices(message.getChunk());

            boolean isSuccessful = chunk.writeChunk(chunkPathPrefix, message.getFilePath(), message.getSequence(),
                    message.getChunk());

            byte status;
            String response;
            if (isSuccessful) {
                this.freeSpace = freeSpace - 64 * 1024;
                status = Protocol.SUCCESS;
                response = "Chunk creation was successful.";

                fileChunksMap.computeIfAbsent(message.getFilePath(), k -> new ArrayList<>()).add(chunk);

                allChunksList.add(chunk.filePath);
                newChunksList.add(chunk.filePath);

                System.out.println(message.getReplicas());

                /* now send the chunk to replicas */
                sendChunkToReplica(message.getFilePath(), message.getSequence(), message.getChunk(),
                        message.getReplicas().get(0),
                        message.getReplicas().get(1), true);
            } else {
                status = Protocol.FAILURE;
                response = "Chunk creation failed.";
            }

            ChunkMessageResponse request = new ChunkMessageResponse(status, response, message.getSequence());
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

    private void sendChunkToReplica(String filePath, int sequence, byte[] chunk, String targetReplica,
            String forwardReplica, boolean forward) {

        try {
            System.out.println(targetReplica);
            System.out.println(forwardReplica);
            Socket socketToPeer = new Socket(targetReplica.split(":")[0], Integer.valueOf(targetReplica.split(":")[1]));
            TCPConnection connection = new TCPConnection(this, socketToPeer);

            CreateReplica request;

            if (forward) {
                request = new CreateReplica(filePath, sequence, chunk,
                        forward, forwardReplica);
            } else {
                request = new CreateReplica(filePath, sequence, chunk,
                        false, "None");
            }

            connection.getTCPSenderThread().sendData(request.getBytes());
            connection.start();

        } catch (IOException | InterruptedException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }

    private void createReplica(CreateReplica message, TCPConnection connection) {
        Chunk chunk = new Chunk(message.getSequence());

        try {
            chunk.createChecksumSlices(message.getChunk());

            boolean isSuccessful = chunk.writeChunk(chunkPathPrefix, message.getFilePath(), message.getSequence(),
                    message.getChunk());

            byte status;
            String response;
            if (isSuccessful) {
                status = Protocol.SUCCESS;
                response = "Chunk replica creation was successful.";

                fileChunksMap.computeIfAbsent(message.getFilePath(), k -> new ArrayList<>()).add(chunk);
                allChunksList.add(chunk.filePath);

                newChunksList.add(chunk.filePath);

                /* now send the chunk to replicas */
                String targetReplica = message.getReplica();
                if (message.checkForward()) {
                    sendChunkToReplica(message.getFilePath(), message.getSequence(), message.getChunk(),
                            targetReplica,
                            "None", false);
                }
            } else {
                status = Protocol.FAILURE;
                response = "Chunk replica creation failed.";
            }

            ChunkMessageResponse request = new ChunkMessageResponse(status, response, message.getSequence());
            connection.getTCPSenderThread().sendData(request.getBytes());

        } catch (Exception e) {
            System.out.println("Error while handling chunk upload" + e.getMessage());
            e.printStackTrace();
        }
    }

    private void handleCreateReplicaResponse(CreateReplicaResponse message, TCPConnection connection) {
        System.out.println("Received chunk replica response from the chunk server: " + message.toString());
        try {
            connection.close();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }

    private void handleRequestChunk(RequestChunk message, TCPConnection connection) {
        /*
         * read the chunk file
         * validate the checksum for each slice
         * if no corruption send the chunk to client
         * if corruption, flag it in the chunk object, which is sent to controller
         * through heartbeat
         * 
         */
        try {
            Chunk chunk = fileChunksMap.get(message.clusterPath).get(message.sequenceNumber - 1);
            File file = new File(message.chunkPath);
            byte[] chunkRead = Files.readAllBytes(file.toPath());

            /* now validate the chunk to see if any corruption exists */
            if (chunk.getDigest(chunkRead).equals(chunk.chunkHash)) {
                RequestChunkResponse response = new RequestChunkResponse(message.downloadPath, message.sequenceNumber,
                        chunkRead, message.totalSize);
                connection.getTCPSenderThread().sendData(response.getBytes());
            } else {
                /*
                 * file is corrupted
                 * find the slice thats corrupted and print it
                 * 
                 */
                List<Integer> indexes = chunk.findCorruptedSlice(chunkRead);
                System.out.println("Found a corrupted chunk for file: " + message.clusterPath + "at chunk sequence: "
                        + message.sequenceNumber + "and at slice indexes: ");
                for (int index : indexes) {
                    System.out.println(index);
                }

                String requestingClientIP = message.requestingClientIP;
                int requestingClientPort = message.requestingClientPort;

                System.out.println("requesting client: " + requestingClientIP + requestingClientPort);
                /* Report chunk corruption to controller */
                ReportChunkCorruption report = new ReportChunkCorruption(fullAddress, message.clusterPath,
                        message.downloadPath, chunk.filePath, false, message.sequenceNumber, message.totalSize,
                        requestingClientIP, requestingClientPort, indexes);

                /*
                 * inform only controller about corruption for correction
                 * inform client to wait
                 */
                connection.getTCPSenderThread().sendData(report.getBytes());
                controllerConnection.getTCPSenderThread().sendData(report.getBytes());

            }

        } catch (Exception e) {
            System.out.println("Error while reading chunk file: " + e.getMessage());
            e.printStackTrace();
        }

    }

    private void handleErrorCorrection(ErrorCorrection message) {
        try {
            Socket socket = new Socket(message.ipAddress, message.port);
            TCPConnection connection = new TCPConnection(this, socket);

            File file = new File(message.filePath);
            byte[] chunkRead = Files.readAllBytes(file.toPath());

            List<byte[]> chunkSlices = getSlices(chunkRead);
            List<byte[]> correctSlices = new ArrayList<>();

            for (int index : message.corruptedSlices) {
                correctSlices.add(chunkSlices.get(index));
            }

            ChunkCorrection response = new ChunkCorrection(message.filePath, message.corruptedSlices, correctSlices);
            connection.getTCPSenderThread().sendData(response.getBytes());
            connection.start();

            /* now send the correct request response to requesting client */
            RequestChunkResponse clientResponse = new RequestChunkResponse(message.downloadPath, message.sequenceNumber,
                    chunkRead, message.totalSize);

            System.out.println(message.requestingClientIP + " " + message.requestingClientPort);

            Socket socketToClient = new Socket(message.requestingClientIP,
                    message.requestingClientPort);
            TCPConnection clientConnection = new TCPConnection(this, socketToClient);
            clientConnection.getTCPSenderThread().sendData(clientResponse.getBytes());
            clientConnection.start();
        } catch (Exception e) {
            System.out.println("Error occurred while sending chunk correction payload: " + e.getMessage());
            e.printStackTrace();
        }

    }

    private void handleChunkCorrection(ChunkCorrection message) {
        /*
         * TODO: first check if digest match then overwrite
         * overwrite the file
         */
        // Chunk chunk = fileChunksMap.get(message.filePath).get(message.sequenceNumber
        // - 1);

        try {
            byte[] corruptedChunk = Files.readAllBytes(Paths.get(message.filePath));
            List<byte[]> corruptSlices = getSlices(corruptedChunk);

            System.out.println(corruptedChunk);
            System.out.println(corruptSlices);
            System.out.println(message.corruptedSliceIndexes);
            System.out.println(message.correctSlices);

            for (int i = 0; i < message.corruptedSliceIndexes.size(); i++) {
                corruptSlices.set(message.corruptedSliceIndexes.get(i), message.correctSlices.get(i));
            }
            /*
             * now corrupt slice has been corrected
             * so write it
             */
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

            for (byte[] slice : corruptSlices) {
                outputStream.write(slice);
            }

            byte[] correctedChunk = outputStream.toByteArray();
            FileOutputStream fileOutputStream = new FileOutputStream(message.filePath);
            fileOutputStream.write(correctedChunk);
            fileOutputStream.close();

            /* now notify controller you are fine */

            ReportChunkCorruption report = new ReportChunkCorruption(fullAddress, "",
                    "", message.filePath, true, 0, 0, "", 0, new ArrayList<>(0));
            controllerConnection.getTCPSenderThread().sendData(report.getBytes());

        } catch (IOException | InterruptedException e) {
            System.err.println("Error overwriting chunk slices: " + e.getMessage());

        }

    }

    private void exitDfs() {
        /*
         * while exiting, send all the files you were responsible for to your successor
         */
        try {
            timerMajorHeartbeat.cancel();
            timerMajorHeartbeat.cancel();
            controllerConnection.close();
        } catch (IOException | InterruptedException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }

    private void handleReplication(ReplicateNewServer message) {
        /* take each chunk and send chunk migration message */
        List<String> chunksList = message.chunksList;
        try {

            Socket socket = new Socket(message.targetIP, message.targetPort);
            TCPConnection connection = new TCPConnection(this, socket);
            connection.start();

            for (String chunkPath : chunksList) {
                File file = new File(chunkPath);
                byte[] chunkRead = Files.readAllBytes(file.toPath());
                String[] parts = chunkPath.replace(chunkPathPrefix, "").split("_chunk");
                String baseFilename = parts[0]; // "demo.txt"
                int sequenceNumber = Integer.valueOf(parts[1]);

                MigrateChunk migrate = new MigrateChunk(sequenceNumber, chunkPath, chunkRead, baseFilename);
                connection.getTCPSenderThread().sendData(migrate.getBytes());

            }
        } catch (IOException | InterruptedException e) {
            System.out.println("Error occurred while migrating chunks " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void handleChunkMigration(MigrateChunk message, TCPConnection connection) {
        /*
         * receive the chunk bytes and write it
         * also add the new chunks to new chunks list, which will be then sent to
         * heartbeats
         */
        Chunk chunk = new Chunk(message.sequenceNumber);

        chunk.createChecksumSlices(message.getChunk());

        boolean isSuccessful = chunk.writeChunk(chunkPathPrefix, message.fileName, message.sequenceNumber,
                message.getChunk());

        byte status;
        String response;
        if (isSuccessful) {
            status = Protocol.SUCCESS;
            response = "Chunk migration was successful.";

            fileChunksMap.computeIfAbsent(message.fileName, k -> new ArrayList<>()).add(chunk);
            allChunksList.add(chunk.filePath);

            newChunksList.add(chunk.filePath);

        } else {
            status = Protocol.FAILURE;
            response = "Chunk migration failed.";
        }
        try {

            MigrationResponse request = new MigrationResponse(status, response, message.sequenceNumber);
            connection.getTCPSenderThread().sendData(request.getBytes());

        } catch (Exception e) {
            System.out.println("Error while handling chunk upload" + e.getMessage());
            e.printStackTrace();
        }

    }

    private void handleMigrationResponse(MigrationResponse message) {
        System.out.println(message.toString());
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

    public long getNumberOfChunks() {
        return numberOfChunks;
    }

    public long getFreeSpace() {
        return freeSpace;
    }

    public List<String> getAllChunks() {
        return allChunksList;

    }

    public List<String> getNewChunks() {
        List<String> result = new ArrayList<>(newChunksList);
        newChunksList.clear();
        return result;
    }

    public List<byte[]> getSlices(byte[] chunk) {
        List<byte[]> slices = new ArrayList<>();

        int offset = 0;
        int sliceSize = 8 * 1024; // 8KB
        int length;

        while (offset < chunk.length) {
            /* keeping length of bytes to read either chunksize or remaining bytes left */
            length = Math.min(sliceSize, chunk.length - offset);
            byte[] slice = new byte[length];
            System.arraycopy(chunk, offset, slice, 0, length);
            slices.add(slice);

            offset += length;
        }

        return slices;
    }

}
