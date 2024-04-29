package csx55.dfs.erasure;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
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
     */
    private final ConcurrentHashMap<String, ChunkServerMetadata> chunkServersMetadata = new ConcurrentHashMap<>();

    /* chunkServer:timestamp */
    private static final ConcurrentHashMap<String, Long> lastHeartbeatTimestamps = new ConcurrentHashMap<>();

    /*
     * this stores location of chunks and its two replicas
     * will be used for retrieve chunk response to client
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
                break;

            case Protocol.FETCH_CHUNKS:
                sendChunks((FetchChunksList) event, connection);
                break;

            case Protocol.REPORT_CHUNK_CORRUPTION:
                handleChunkCorruption((ReportChunkCorruption) event);

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
        System.out.println("received major heartbeat");
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
    }

    private synchronized void handleMinorHeartbeat(MinorHeartbeat message) {
        System.out.println("received minor heartbeat");
        // send metadata info of newly added chunks
        // report file corruption if detected in this heart beat

        ChunkServerMetadata metadata = chunkServersMetadata.get(message.chunkServerString);
        metadata.updateFreeSpace(message.freeSpace);
        metadata.updateChunksCount(message.numberOfChunks);
        metadata.appendChunksList(message.newChunksList);

        /* now for each obtained new chunk update the chunk and server map */
        for (String chunkPath : message.newChunksList) {
            chunkAndServerMap.computeIfAbsent(chunkPath, k -> new ArrayList<>()).add(metadata);

        }

        if (!startedMonitoring) {
            startHeartBeatMonitoring();

        }

    }

    private void sendChunkServers(FetchChunkServers request, TCPConnection connection) {
        /*
         * TODO: get all the chunkservers that have free space at least the size of the
         * file
         */
        List<String> fileChunkServers = new ArrayList<>();

        /*
         * TODO: get 3 free chunk servers
         * this is for testing only
         */
        List<Map.Entry<String, ChunkServerMetadata>> sortedServers = new ArrayList<>(chunkServersMetadata.entrySet());
        Collections.sort(sortedServers,
                (a, b) -> Long.compare(b.getValue().getFreeSpace(), a.getValue().getFreeSpace()));
        List<Map.Entry<String, ChunkServerMetadata>> topThreeServers = sortedServers.subList(0,
                Math.min(3, sortedServers.size()));

        for (Map.Entry<String, ChunkServerMetadata> entry : topThreeServers) {
            fileChunkServers.add(entry.getKey());
        }

        try {
            ChunkServerList message = new ChunkServerList(fileChunkServers,
                    request.getDestinationPath(), request.getSequence());
            connection.getTCPSenderThread().sendData(message.getBytes());

        } catch (Exception e) {
            System.out.println("Error sending chunk servers list: " + e.getMessage());
            e.printStackTrace();
        }

    }

    private void sendChunks(FetchChunksList message, TCPConnection connection) {
        try {
            /*
             * from the chunkAndServerMap, get all the chunks that contain the clusterPath
             * substring
             */
            List<String> keysContainingClusterPath = chunkAndServerMap.keySet().stream()
                    .filter(key -> key.contains(message.clusterPath))
                    .collect(Collectors.toList());

            List<String> validChunkServers = new ArrayList<>();

            for (String chunk : keysContainingClusterPath) {
                String targetServer = getValidChunkServer(chunkAndServerMap.get(chunk), message.clusterPath);
                validChunkServers.add(targetServer);
            }

            FetchChunksListResponse response = new FetchChunksListResponse(keysContainingClusterPath.size(),
                    keysContainingClusterPath, validChunkServers, message.clusterPath, message.downloadPath);
            connection.getTCPSenderThread().sendData(response.getBytes());
        } catch (Exception e) {
            System.out.println("Error while sending fetch chunks list request: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private String getValidChunkServer(List<ChunkServerMetadata> servers, String chunkFile) throws Exception {
        for (ChunkServerMetadata server : servers) {
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
                ErrorCorrection payload = new ErrorCorrection(message.chunkPath,
                        message.originChunkServer.split(":")[0],
                        Integer.valueOf(message.originChunkServer.split(":")[1]));
                TCPConnection connection = chunkServersMetadata.get(validServer).getConnection();
                connection.getTCPSenderThread().sendData(payload.getBytes());
            }

        } catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }

    }

    public void startHeartBeatMonitoring() {
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(new TimerTask() {
            public void run() {
                checkHeartbeat();
            }
        }, 0, 20 * 1000);
    }

    private void checkHeartbeat() {
        long currentTimestamp = System.currentTimeMillis();
        for (Map.Entry<String, Long> entry : lastHeartbeatTimestamps.entrySet()) {
            String serverId = entry.getKey();
            long lastHeartbeatTime = entry.getValue();
            if (currentTimestamp - lastHeartbeatTime > 20 * 1000) {
                handleFailure(serverId);
            }
        }
    }

    private void handleFailure(String chunkServer) {
        /*
         * first remove this chunkserver from all the data structures
         * replicate to new node to handle failure of this node
         */
    }

}
