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

public class ChunkMessage implements Event {
    /* filepath is the destination path */
    private String filePath;
    private int type;
    private byte[] chunk;
    private int sequenceNumber;
    private List<String> replicas;

    public ChunkMessage(String filePath, int sequenceNumber, byte[] chunk,
            List<String> replicas) {
        this.type = Protocol.CHUNK_TRANSFER;
        this.filePath = filePath;
        this.chunk = chunk;
        this.sequenceNumber = sequenceNumber;
        this.replicas = replicas;
    }

    public ChunkMessage(byte[] marshalledData) throws IOException {
        ByteArrayInputStream inputData = new ByteArrayInputStream(marshalledData);

        DataInputStream din = new DataInputStream(new BufferedInputStream(inputData));

        this.type = din.readInt();

        this.sequenceNumber = din.readInt();

        int len = din.readInt();
        byte[] data = new byte[len];
        din.readFully(data);
        this.filePath = new String(data);

        len = din.readInt();
        byte[] payloadData = new byte[len];
        din.readFully(payloadData);
        this.chunk = payloadData;

        this.replicas = new ArrayList<String>(2);
        for (int i = 0; i < 2; i++) {
            len = din.readInt();
            data = new byte[len];
            din.readFully(data);
            this.replicas.add(new String(data));
        }

        inputData.close();
        din.close();

    }

    public byte[] getBytes() throws IOException {
        byte[] marshalledData;
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(outputStream));

        dout.writeInt(type);

        dout.writeInt(sequenceNumber);

        dout.writeInt(filePath.getBytes().length);
        dout.write(filePath.getBytes());

        dout.writeInt(this.chunk.length);
        dout.write(this.chunk);

        for (String server : replicas) {
            byte[] bytes = server.getBytes();
            dout.writeInt(bytes.length);
            dout.write(bytes);
        }

        dout.flush();
        marshalledData = outputStream.toByteArray();

        outputStream.close();
        dout.close();

        return marshalledData;
    }

    public byte[] getChunk() {
        return chunk;
    }

    public String getFilePath() {
        return filePath;
    }

    public int getType() {
        return type;
    }

    public int getSequence() {
        return sequenceNumber;
    }

    public List<String> getReplicas() {
        return replicas;
    }

}
