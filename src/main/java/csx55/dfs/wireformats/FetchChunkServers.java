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

    private String sourcePath;
    private String destinationPath;

    public FetchChunkServers(String sourcePath, String destinationPath) {
        this.type = Protocol.FETCH_CHUNK_SERVERS;
        this.sourcePath = sourcePath;
        this.destinationPath = destinationPath;
    }

    public FetchChunkServers(byte[] marshalledData) throws IOException {

        ByteArrayInputStream inputData = new ByteArrayInputStream(marshalledData);

        DataInputStream din = new DataInputStream(new BufferedInputStream(inputData));

        this.type = din.readInt();

        int len = din.readInt();
        byte[] stringData = new byte[len];
        din.readFully(stringData, 0, len);
        this.sourcePath = new String(stringData);

        len = din.readInt();
        stringData = new byte[len];
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

        byte[] stringBytes = sourcePath.getBytes();
        dout.writeInt(stringBytes.length);
        dout.write(stringBytes);

        stringBytes = destinationPath.getBytes();
        dout.writeInt(stringBytes.length);
        dout.write(stringBytes);

        dout.flush();
        marshalledData = outputStream.toByteArray();

        outputStream.close();
        dout.close();
        return marshalledData;

    }

    public String getSourcePath() {
        return sourcePath;
    }

    public String getDestinationPath() {
        return destinationPath;
    }

}