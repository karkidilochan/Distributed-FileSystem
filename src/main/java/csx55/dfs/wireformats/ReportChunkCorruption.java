package csx55.dfs.wireformats;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ReportChunkCorruption implements Event {
    private int type;
    public String clusterPath;
    public String downloadPath;
    public String originChunkServer;
    public String chunkPath;
    public boolean isFixed;
    public int sequenceNumber;
    public int totalSize;
    public String requestingClientIP;
    public int requestingClientPort;
    public List<Integer> corruptedSlices;

    public ReportChunkCorruption(String originChunkServer, String clusterPath, String downloadPath, String chunkPath,
            boolean isFixed, int sequenceNumber, int totalSize, String requestingClient, int requestingClientPort,
            List<Integer> corruptedSlices) {
        this.type = Protocol.REPORT_CHUNK_CORRUPTION;
        this.clusterPath = clusterPath;
        this.downloadPath = downloadPath;
        this.chunkPath = chunkPath;
        this.originChunkServer = originChunkServer;
        this.isFixed = isFixed;
        this.sequenceNumber = sequenceNumber;
        this.totalSize = totalSize;
        this.requestingClientIP = requestingClient;
        this.requestingClientPort = requestingClientPort;
        this.corruptedSlices = corruptedSlices;
    }

    public ReportChunkCorruption(byte[] marshalledData) throws IOException {
        ByteArrayInputStream inputData = new ByteArrayInputStream(marshalledData);

        DataInputStream din = new DataInputStream(new BufferedInputStream(inputData));

        this.type = din.readInt();

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
        this.originChunkServer = new String(data);

        this.isFixed = din.readBoolean();

        this.sequenceNumber = din.readInt();
        this.totalSize = din.readInt();

        len = din.readInt();
        data = new byte[len];
        din.readFully(data);
        this.requestingClientIP = new String(data);

        this.requestingClientPort = din.readInt();

        len = din.readInt();
        this.corruptedSlices = new ArrayList<>(len);
        for (int i = 0; i < len; i++) {
            int index = din.readInt();
            this.corruptedSlices.add(index);
        }

        inputData.close();
        din.close();

    }

    public byte[] getBytes() throws IOException {
        byte[] marshalledData;
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(outputStream));

        dout.writeInt(type);

        dout.writeInt(clusterPath.getBytes().length);
        dout.write(clusterPath.getBytes());

        dout.writeInt(downloadPath.getBytes().length);
        dout.write(downloadPath.getBytes());

        dout.writeInt(chunkPath.getBytes().length);
        dout.write(chunkPath.getBytes());

        dout.writeInt(originChunkServer.getBytes().length);
        dout.write(originChunkServer.getBytes());

        dout.writeBoolean(isFixed);

        dout.writeInt(sequenceNumber);
        dout.writeInt(totalSize);

        dout.writeInt(requestingClientIP.getBytes().length);
        dout.write(requestingClientIP.getBytes());

        dout.writeInt(requestingClientPort);

        dout.writeInt(corruptedSlices.size());
        for (int slice : corruptedSlices) {
            dout.writeInt(slice);
        }

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
