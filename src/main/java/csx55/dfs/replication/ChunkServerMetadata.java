package csx55.dfs.replication;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import csx55.dfs.tcp.TCPConnection;

public class ChunkServerMetadata {
    public String ipAddress;
    public int port;
    public long freeSpace;
    public TCPConnection connection;
    public long chunksCount;

    /* hashmap of chunk file and its invalidity */
    public Map<String, Boolean> fileChunksList = new HashMap<>();
    public boolean isAlive = true;

    public ChunkServerMetadata() {

    }

    public ChunkServerMetadata(String ipAddress, int port, TCPConnection connection) {
        this.ipAddress = ipAddress;
        this.port = port;
        this.connection = connection;
    }

    public void updateFreeSpace(long data) {
        freeSpace = data;
    }

    public void updateChunksList(List<String> newList) {
        for (String newItem : newList) {
            fileChunksList.computeIfAbsent(newItem, k -> false);
        }
    }

    public void appendChunksList(List<String> appendList) {
        for (String newItem : appendList) {
            fileChunksList.put(newItem, false);
        }
    }

    public int getNumberOfChunks() {
        return fileChunksList.size();
    }

    public TCPConnection getConnection() {
        return connection;
    }

    public void updateChunksCount(long data) {
        chunksCount = data;
    }

    public String getFullAddress() {
        return ipAddress + ":" + port;
    }

    public long getFreeSpace() {
        return freeSpace;
    }

}
