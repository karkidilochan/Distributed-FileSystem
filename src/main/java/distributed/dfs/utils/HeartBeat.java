package distributed.dfs.utils;

import java.util.TimerTask;
import java.io.IOException;

import distributed.dfs.replication.ChunkServer;
import distributed.dfs.tcp.TCPConnection;
import distributed.dfs.wireformats.MajorHeartbeat;
import distributed.dfs.wireformats.MinorHeartbeat;

public class HeartBeat extends TimerTask {
    private final boolean isMajor;
    private final TCPConnection controllerConnection;

    private ChunkServer chunkServer;

    public HeartBeat(boolean isMajor, TCPConnection controllerConnection, ChunkServer chunkServer) {
        this.isMajor = isMajor;
        this.controllerConnection = controllerConnection;
        this.chunkServer = chunkServer;
    }

    public void run() {
        try {
            byte[] data;
            if (isMajor) {
                data = prepareMajorHeartbeat();
            } else {
                data = prepareMinorHeartbeat();
            }
            controllerConnection.getTCPSenderThread().sendData(data);
        } catch (IOException | InterruptedException e) {
            System.out.println("Error occurred while sending heartbeat message: " + e.getMessage());
            e.printStackTrace();
        }

    }

    private byte[] prepareMajorHeartbeat() throws IOException {
        MajorHeartbeat message = new MajorHeartbeat(chunkServer.getFullAddress(), chunkServer.getNumberOfChunks(),
                chunkServer.getFreeSpace(), chunkServer.getAllChunks());
        return message.getBytes();
    }

    private byte[] prepareMinorHeartbeat() throws IOException {
        MinorHeartbeat message = new MinorHeartbeat(chunkServer.getFullAddress(), chunkServer.getNumberOfChunks(),
                chunkServer.getFreeSpace(), chunkServer.getNewChunks());
        return message.getBytes();
    }
}
