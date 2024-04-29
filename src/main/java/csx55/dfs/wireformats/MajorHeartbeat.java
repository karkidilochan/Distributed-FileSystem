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

public class MajorHeartbeat implements Event {
    private int type;
    public String chunkServerString;
    public long numberOfChunks;
    public long freeSpace;
    public List<String> chunksList = new ArrayList<>();

    public MajorHeartbeat(String chunkServerString, long numberOfChunks, long freeSpace, List<String> chunksList) {
        this.type = Protocol.MAJOR_HEARTBEAT;
        this.chunkServerString = chunkServerString;
        this.numberOfChunks = numberOfChunks;
        this.freeSpace = freeSpace;
        this.chunksList = chunksList;
    }

    public MajorHeartbeat(byte[] marshalledData) throws IOException {
        ByteArrayInputStream inputData = new ByteArrayInputStream(marshalledData);

        // wrap internal bytes array with data input stream
        DataInputStream din = new DataInputStream(new BufferedInputStream(inputData));

        this.type = din.readInt();

        this.numberOfChunks = din.readLong();
        this.freeSpace = din.readLong();

        this.chunksList = new ArrayList<String>();
        for (int i = 0; i < numberOfChunks; i++) {
            int len = din.readInt();
            byte[] stringData = new byte[len];
            din.readFully(stringData);
            this.chunksList.add(new String(stringData));
        }

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

        dout.writeLong(numberOfChunks);
        dout.writeLong(freeSpace);

        for (String chunk : chunksList) {
            byte[] bytes = chunk.getBytes();
            dout.writeInt(bytes.length);
            dout.write(bytes);
        }

        dout.flush();
        marshalledData = outputStream.toByteArray();

        outputStream.close();
        dout.close();
        return marshalledData;

    }

}
