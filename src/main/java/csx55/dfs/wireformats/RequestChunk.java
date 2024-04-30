package csx55.dfs.wireformats;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class RequestChunk implements Event {
    /* filepath is the destination path */
    private int type;
    public String clusterPath;
    public String downloadPath;
    public String chunkPath;
    public int sequenceNumber;
    public int totalSize;
    public String requestingClientIP;
    public int requestingClientPort;

    public RequestChunk(String clusterPath, String downloadPath, String chunkPath, int sequenceNumber, int totalSize,
            String requestingClientIP, int requestingClientPort) {
        this.type = Protocol.REQUEST_CHUNK;
        this.clusterPath = clusterPath;
        this.downloadPath = downloadPath;
        this.chunkPath = chunkPath;
        this.sequenceNumber = sequenceNumber;
        this.totalSize = totalSize;
        this.requestingClientIP = requestingClientIP;
        this.requestingClientPort = requestingClientPort;
    }

    public RequestChunk(byte[] marshalledData) throws IOException {
        ByteArrayInputStream inputData = new ByteArrayInputStream(marshalledData);

        DataInputStream din = new DataInputStream(new BufferedInputStream(inputData));

        this.type = din.readInt();

        this.sequenceNumber = din.readInt();
        this.totalSize = din.readInt();

        int len = din.readInt();
        byte[] data = new byte[len];
        din.readFully(data);
        this.clusterPath = new String(data);

        len = din.readInt();
        data = new byte[len];
        din.readFully(data);
        this.downloadPath = new String(data);

        len = din.readInt();
        data = new byte[len];
        din.readFully(data);
        this.chunkPath = new String(data);

        len = din.readInt();
        data = new byte[len];
        din.readFully(data);
        this.requestingClientIP = new String(data);

        this.requestingClientPort = din.readInt();

        inputData.close();
        din.close();

    }

    public byte[] getBytes() throws IOException {
        byte[] marshalledData;
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(outputStream));

        dout.writeInt(type);

        dout.writeInt(sequenceNumber);
        dout.writeInt(totalSize);

        dout.writeInt(clusterPath.getBytes().length);
        dout.write(clusterPath.getBytes());

        dout.writeInt(downloadPath.getBytes().length);
        dout.write(downloadPath.getBytes());

        dout.writeInt(chunkPath.getBytes().length);
        dout.write(chunkPath.getBytes());

        dout.writeInt(requestingClientIP.getBytes().length);
        dout.write(requestingClientIP.getBytes());

        dout.writeInt(requestingClientPort);

        dout.flush();
        marshalledData = outputStream.toByteArray();

        outputStream.close();
        dout.close();

        return marshalledData;
    }

    public int getType() {
        return type;
    }

}
