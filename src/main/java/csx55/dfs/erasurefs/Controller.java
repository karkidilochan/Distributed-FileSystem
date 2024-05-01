package csx55.dfs.erasurefs;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import csx55.dfs.tcp.TCPConnection;
import csx55.dfs.tcp.TCPServer;
import csx55.dfs.utils.Node;
import csx55.dfs.wireformats.Event;
import csx55.dfs.wireformats.FetchChunkServers;
import csx55.dfs.wireformats.FetchChunksList;
import csx55.dfs.wireformats.FetchChunksListResponse;
import csx55.dfs.wireformats.Protocol;
import csx55.dfs.wireformats.Register;
import csx55.dfs.wireformats.RegisterResponse;
import csx55.dfs.wireformats.ReplicateNewServer;
import csx55.dfs.wireformats.ReportChunkCorruption;
import csx55.dfs.wireformats.ChunkServerList;
import csx55.dfs.wireformats.ErrorCorrection;
import csx55.dfs.wireformats.MajorHeartbeat;
import csx55.dfs.wireformats.MinorHeartbeat;

public class Controller implements Node {
    // Constants representing different commands

    private Map<String, TCPConnection> clientConnections = new HashMap<>();

    /*
     * stores reference to metadata for each chunkserver
     * this will be used to update the metadata by heartbeats
     * chunkServerString:metadata
     */
    private final ConcurrentHashMap<String, ChunkServerMetadata> chunkServersMetadata = new ConcurrentHashMap<>();

    /* chunkServer:timestamp */
    private static final ConcurrentHashMap<String, Long> lastHeartbeatTimestamps = new ConcurrentHashMap<>();

    /*
     * this stores location of chunks and its two replicas
     * will be used for retrieve chunk response to client
     * chunkPath:replicas
     */
    private final ConcurrentHashMap<String, List<ChunkServerMetadata>> chunkAndServerMap = new ConcurrentHashMap<>();

    private boolean startedMonitoring = false;

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
        // System.out.println("Received event: " + event.toString());

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

            case Protocol.FETCH_CHUNK_SERVERS:
                sendChunkServers((FetchChunkServers) event, connection);
                break;

            case Protocol.FETCH_CHUNKS:
                sendChunks((FetchChunksList) event, connection);
                break;

            case Protocol.REPORT_CHUNK_CORRUPTION:
                handleChunkCorruption((ReportChunkCorruption) event);
                break;

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
            ChunkServerMetadata metadata = new ChunkServerMetadata(ipAddress, registerEvent.getHostPort(), connection);
            chunkServersMetadata.put(nodes, metadata);

            lastHeartbeatTimestamps.put(nodes, System.currentTimeMillis());

            message = "Registration request successful.  The number of chunk servers currently is ("
                    + chunkServersMetadata.size() + ").\n";
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
            chunkServersMetadata.remove(nodes);
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

    private synchronized void handleMajorHeartbeat(MajorHeartbeat message) {

        try {

            lastHeartbeatTimestamps.put(message.chunkServerString, System.currentTimeMillis());

            ChunkServerMetadata metadata = chunkServersMetadata.get(message.chunkServerString);
            metadata.updateFreeSpace(message.freeSpace);
            metadata.updateChunksCount(message.numberOfChunks);
            metadata.updateChunksList(message.chunksList);

            /* now for each obtained all chunk update the chunk and server map */
            for (String chunkPath : message.chunksList) {
                if (chunkAndServerMap.containsKey(chunkPath)) {
                    List<ChunkServerMetadata> metadataList = chunkAndServerMap.get(chunkPath);
                    if (!metadataList.contains(metadata)) {
                        metadataList.add(metadata);
                    }
                } else {
                    List<ChunkServerMetadata> list = new ArrayList<>();
                    list.add(metadata);
                    chunkAndServerMap.put(chunkPath, list);
                }

            }

            // send metadata of all chunks
            // plus, total no of chunks, free space available(1GB - space used so far)
        } catch (Exception e) {
            System.out.println("Error occurred while handling major heartbeat: " + e.getMessage());
            e.printStackTrace();
        }

    }

    private synchronized void handleMinorHeartbeat(MinorHeartbeat message) {
        // send metadata info of newly added chunks
        // report file corruption if detected in this heart beat
        lastHeartbeatTimestamps.put(message.chunkServerString, System.currentTimeMillis());

        ChunkServerMetadata metadata = chunkServersMetadata.get(message.chunkServerString);
        metadata.updateFreeSpace(message.freeSpace);
        metadata.updateChunksCount(message.numberOfChunks);
        metadata.appendChunksList(message.newChunksList);

        /* now for each obtained new chunk update the chunk and server map */
        for (String chunkPath : message.newChunksList) {
            chunkAndServerMap.computeIfAbsent(chunkPath, k -> new ArrayList<>()).add(metadata);

        }

        if (!startedMonitoring) {
            System.out.println("Started monitoring hearbeat");
            startHeartBeatMonitoring();
            startedMonitoring = true;

        }

    }

    private void sendChunkServers(FetchChunkServers request, TCPConnection connection) {

        List<String> fileChunkServers = new ArrayList<>();

        List<Map.Entry<String, ChunkServerMetadata>> sortedServers = new ArrayList<>(chunkServersMetadata.entrySet());
        Collections.sort(sortedServers,
                (a, b) -> Long.compare(b.getValue().getFreeSpace(), a.getValue().getFreeSpace()));
        List<Map.Entry<String, ChunkServerMetadata>> topThreeServers = sortedServers.subList(0,
                Math.min(1, sortedServers.size()));

        System.out.println("sortedServers " + sortedServers);
        System.out.println(topThreeServers);

        for (Map.Entry<String, ChunkServerMetadata> entry : topThreeServers) {
            fileChunkServers.add(entry.getKey());

        }

        System.out.println("fileChunkServers: " + fileChunkServers);

        try {
            ChunkServerList message = new ChunkServerList(fileChunkServers,
                    request.getDestinationPath(), request.getSequence());
            connection.getTCPSenderThread().sendData(message.getBytes());

        } catch (Exception e) {
            System.out.println("Error sending chunk servers list: " + e.getMessage());
            e.printStackTrace();
        }

        System.out.println("sent chunk servers list to client");

    }

    private void sendChunks(FetchChunksList message, TCPConnection connection) {
        try {
            /*
             * from the chunkAndServerMap, get all the chunks that contain the clusterPath
             * substring
             */
            /*
             * {/tmp/chunk-server/demo.txt_chunk1=[csx55.dfs.replication.ChunkServerMetadata
             * 
             * @7989edb3, csx55.dfs.replication.ChunkServerMetadata@7989edb3,
             * csx55.dfs.replication.ChunkServerMetadata@7989edb3]}
             * demo.txt
             * [/tmp/chunk-server/demo.txt_chunk1]
             * {/tmp/chunk-server/demo.txt_chunk1=false}
             * [129.82.44.157:41555]
             */

            System.out.println(chunkAndServerMap);

            List<String> keysContainingClusterPath = chunkAndServerMap.keySet().stream()
                    .filter(key -> key.contains(message.clusterPath))
                    .collect(Collectors.toList());

            System.out.println(message.clusterPath);
            System.out.println(keysContainingClusterPath);

            List<String> validChunkServers = new ArrayList<>();

            for (String chunk : keysContainingClusterPath) {
                String targetServer = getValidChunkServer(chunkAndServerMap.get(chunk), chunk);
                validChunkServers.add(targetServer);
            }

            System.out.println(validChunkServers);

            FetchChunksListResponse response = new FetchChunksListResponse(keysContainingClusterPath.size(),
                    keysContainingClusterPath, validChunkServers, message.clusterPath, message.downloadPath);
            connection.getTCPSenderThread().sendData(response.getBytes());
        } catch (Exception e) {
            System.out.println("Error while responding to fetch chunks list request: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private String getValidChunkServer(List<ChunkServerMetadata> servers, String chunkFile) throws Exception {

        for (ChunkServerMetadata server : servers) {
            System.out.println(server.fileChunksList);
            boolean checkIfFileInvalid = server.fileChunksList.get(chunkFile);
            if (server.isAlive && !checkIfFileInvalid) {
                return server.getFullAddress();
            }
        }
        throw new Exception("Can't find valid chunk server for the chunk");
    }

    private void handleChunkCorruption(ReportChunkCorruption message) {
        /*
         * find another replica of this chunk
         * send that replica a command to send correct slice of that index to this
         * corrupted server
         * perform error correction of the chunk slice
         */
        try {
            ChunkServerMetadata server = chunkServersMetadata.get(message.originChunkServer);

            if (message.isFixed) {
                /* boolean for invalidity */
                server.fileChunksList.put(message.chunkPath, false);
            } else {
                /* boolean for invalidity */
                server.fileChunksList.put(message.chunkPath, true);
                String validServer = getValidChunkServer(chunkAndServerMap.get(message.chunkPath), message.chunkPath);

                System.out.println(validServer);
                System.out.println(server.ipAddress + server.port);
                ErrorCorrection payload = new ErrorCorrection(message.chunkPath,
                        server.ipAddress,
                        server.port, message.clusterPath, message.downloadPath, message.sequenceNumber,
                        message.totalSize, message.requestingClientIP, message.requestingClientPort,
                        message.corruptedSlices);
                TCPConnection connection = chunkServersMetadata.get(validServer).getConnection();
                connection.getTCPSenderThread().sendData(payload.getBytes());
            }

        } catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }

    }

    public void startHeartBeatMonitoring() {
        // Timer timer = new Timer();
        System.out.println("Started Timer");
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

        executor.scheduleAtFixedRate(this::checkHeartbeat, 0, 20, TimeUnit.SECONDS);

        // timer.scheduleAtFixedRate(new TimerTask() {
        // public void run() {
        // checkHeartbeat();
        // }
        // }, 0, 20 * 1000);
    }

    private void checkHeartbeat() {

        long currentTimestamp = System.currentTimeMillis();
        for (Map.Entry<String, Long> entry : lastHeartbeatTimestamps.entrySet()) {
            String serverId = entry.getKey();
            long lastHeartbeatTime = entry.getValue();
            if (currentTimestamp - lastHeartbeatTime > 20 * 1000) {
                System.out.println("Detected failure of chunkserver: " + serverId);
                handleFailure(serverId);
            }
        }
    }

    private synchronized void handleFailure(String failedChunkServer) {
        /*
         * first remove this chunkserver from all the data structures
         * replicate to new node to handle failure of this node
         * Approach for this,
         * find all the chunks this chunk server was holding
         * then for each chunk, find the chunk servers that contain replicas
         * find a chunk server that doesn't contain replica of this chunk
         * then instruct this replica to send a copy of that chunk to this new chunk
         * server
         */
        try {
            ChunkServerMetadata failedMetadata = chunkServersMetadata.get(failedChunkServer);
            failedMetadata.isAlive = false;
            /* chunkserver:affectedChunk */
            Map<ChunkServerMetadata, List<String>> affectedChunksReplicaMap = new HashMap<>();
            ChunkServerMetadata bestCandidate = new ChunkServerMetadata();
            for (Map.Entry<String, List<ChunkServerMetadata>> entry : chunkAndServerMap.entrySet()) {
                List<ChunkServerMetadata> replicasList = entry.getValue();
                if (replicasList.contains(failedMetadata)) {
                    List<ChunkServerMetadata> candidateServers = chunkServersMetadata.values().stream()
                            .filter(metadata -> !replicasList.contains(metadata))
                            .collect(Collectors.toList());

                    replicasList.remove(failedMetadata);
                    chunkServersMetadata.remove(failedChunkServer);
                    lastHeartbeatTimestamps.remove(failedChunkServer);

                    candidateServers.sort((m1, m2) -> Long.compare(m2.getFreeSpace(), m1.getFreeSpace()));

                    bestCandidate = candidateServers.get(0);

                    affectedChunksReplicaMap.computeIfAbsent(replicasList.get(0), k -> new ArrayList<>())
                            .add(entry.getKey());

                    /* remove failed server from all data structures */

                }

            }
            System.out.println(affectedChunksReplicaMap);
            System.out.println(bestCandidate.getFullAddress());

            /*
             * now for affectedChunksReplicaMap entries, in a loop
             * send ReplicateNewServer to the replica server, payload contains best
             * candidate
             * replica server will send the chunk to best candidate
             * best candidate saves it, send the newchunkslist in minor heartbeat, ->
             * updated in controller
             */
            for (Map.Entry<ChunkServerMetadata, List<String>> entry : affectedChunksReplicaMap.entrySet()) {
                ChunkServerMetadata replicaServer = entry.getKey();
                System.out.println("Sending to: " + replicaServer.getFullAddress());
                List<String> chunksList = entry.getValue();
                ReplicateNewServer response = new ReplicateNewServer(bestCandidate.ipAddress, bestCandidate.port,
                        chunksList);

                try {

                    replicaServer.connection.getTCPSenderThread().sendData(response.getBytes());
                } catch (Exception e) {
                    System.out.println("Error occurred while sending replicate server message to "
                            + replicaServer.getFullAddress() + ": " + e.getMessage());
                    e.printStackTrace();

                }

            }
            System.out.println("sent replication requests");
        } catch (Exception e) {
            System.out.println("Error occurred while handling failure of chunk server and migrating affected chunks"
                    + e.getMessage());
        }

    }

}
