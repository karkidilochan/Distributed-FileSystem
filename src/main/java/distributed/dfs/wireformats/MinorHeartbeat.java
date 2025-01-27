package distributed.dfs.wireformats;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import java.util.ArrayList;
import java.util.List;

public class MinorHeartbeat implements Event {
    private int type;

    public long numberOfChunks;
    public long freeSpace;
    public List<String> newChunksList = new ArrayList<>();
    public String chunkServerString;

    public MinorHeartbeat(String chunkServerString, long numberOfChunks, long freeSpace,
            List<String> newChunksList) {
        this.type = Protocol.MINOR_HEARTBEAT;
        this.chunkServerString = chunkServerString;
        this.numberOfChunks = numberOfChunks;
        this.freeSpace = freeSpace;
        this.newChunksList = newChunksList;

    }

    public MinorHeartbeat(byte[] marshalledData) throws IOException {
        ByteArrayInputStream inputData = new ByteArrayInputStream(marshalledData);

        // wrap internal bytes array with data input stream
        DataInputStream din = new DataInputStream(new BufferedInputStream(inputData));

        this.type = din.readInt();

        int len = din.readInt();
        byte[] data = new byte[len];
        din.readFully(data);
        this.chunkServerString = new String(data);

        this.numberOfChunks = din.readLong();
        this.freeSpace = din.readLong();

        long size = din.readInt();
        this.newChunksList = new ArrayList<String>();
        for (int i = 0; i < size; i++) {
            len = din.readInt();
            byte[] stringData = new byte[len];
            din.readFully(stringData);
            this.newChunksList.add(new String(stringData));
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

        dout.writeInt(chunkServerString.getBytes().length);
        dout.write(chunkServerString.getBytes());

        dout.writeLong(numberOfChunks);
        dout.writeLong(freeSpace);

        dout.writeInt(newChunksList.size());
        for (String chunk : newChunksList) {
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
