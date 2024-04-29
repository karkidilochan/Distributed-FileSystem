package csx55.dfs.wireformats;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class FetchChunkServers implements Event {
    private int type;

    private String destinationPath;
    private int sequenceNumber;
    private long fileSize;

    public FetchChunkServers(String destinationPath, int sequence, long fileSize) {
        this.type = Protocol.FETCH_CHUNK_SERVERS;
        this.destinationPath = destinationPath;
        this.sequenceNumber = sequence;
        this.fileSize = fileSize;
    }

    public FetchChunkServers(byte[] marshalledData) throws IOException {

        ByteArrayInputStream inputData = new ByteArrayInputStream(marshalledData);

        DataInputStream din = new DataInputStream(new BufferedInputStream(inputData));

        this.type = din.readInt();

        this.sequenceNumber = din.readInt();
        this.fileSize = din.readLong();

        int len = din.readInt();
        byte[] stringData = new byte[len];
        din.readFully(stringData, 0, len);
        this.destinationPath = new String(stringData);

        inputData.close();
        din.close();
    }

    public int getType() {
        return type;
    }

    public byte[] getBytes() throws IOException {
        byte[] marshalledData;
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(outputStream));

        dout.writeInt(type);

        dout.writeInt(sequenceNumber);
        dout.writeLong(fileSize);

        byte[] stringBytes = destinationPath.getBytes();
        dout.writeInt(stringBytes.length);
        dout.write(stringBytes);

        dout.flush();
        marshalledData = outputStream.toByteArray();

        outputStream.close();
        dout.close();
        return marshalledData;

    }

    public String getDestinationPath() {
        return destinationPath;
    }

    public int getSequence() {
        return sequenceNumber;
    }

    public long getFileSize() {
        return fileSize;
    }

}