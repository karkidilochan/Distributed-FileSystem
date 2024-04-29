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

public class FetchChunksList implements Event {
    private int type;

    public String downloadPath;
    public String clusterPath;

    public FetchChunksList(String clusterPath,
            String downloadPath) {
        this.type = Protocol.FETCH_CHUNKS;
        this.clusterPath = clusterPath;
        this.downloadPath = downloadPath;
    }

    public FetchChunksList(byte[] marshalledData) throws IOException {
        // creating input stream to read byte data sent over network connection
        ByteArrayInputStream inputData = new ByteArrayInputStream(marshalledData);

        // wrap internal bytes array with data input stream
        DataInputStream din = new DataInputStream(new BufferedInputStream(inputData));

        this.type = din.readInt();

        int len;
        byte[] stringData;

        len = din.readInt();
        stringData = new byte[len];
        din.readFully(stringData, 0, len);
        this.clusterPath = new String(stringData);

        len = din.readInt();
        stringData = new byte[len];
        din.readFully(stringData, 0, len);
        this.downloadPath = new String(stringData);

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

        byte[] stringBytes;

        stringBytes = clusterPath.getBytes();
        dout.writeInt(stringBytes.length);
        dout.write(stringBytes);

        stringBytes = downloadPath.getBytes();
        dout.writeInt(stringBytes.length);
        dout.write(stringBytes);

        dout.flush();
        marshalledData = outputStream.toByteArray();

        outputStream.close();
        dout.close();
        return marshalledData;

    }

    public String getDestinationPath() {
        return downloadPath;
    }

    public String getSourcePath() {
        return downloadPath;
    }

}