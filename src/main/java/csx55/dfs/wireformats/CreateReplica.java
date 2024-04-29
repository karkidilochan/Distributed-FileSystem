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

public class CreateReplica implements Event {
    /* filepath is the destination path */
    private String filePath;
    private int type;
    private byte[] chunk;
    private int sequenceNumber;
    private String replica;
    private boolean forward;

    public CreateReplica(String filePath, int sequenceNumber, byte[] chunk,
            boolean forward, String replica) {
        this.type = Protocol.CREATE_REPLICA;
        this.filePath = filePath;
        this.chunk = chunk;
        this.sequenceNumber = sequenceNumber;
        this.replica = replica;
        this.forward = forward;
    }

    public CreateReplica(byte[] marshalledData) throws IOException {
        ByteArrayInputStream inputData = new ByteArrayInputStream(marshalledData);

        DataInputStream din = new DataInputStream(new BufferedInputStream(inputData));

        this.type = din.readInt();

        this.forward = din.readBoolean();

        this.sequenceNumber = din.readInt();

        int len = din.readInt();
        byte[] data = new byte[len];
        din.readFully(data);
        this.filePath = new String(data);

        len = din.readInt();
        byte[] payloadData = new byte[len];
        din.readFully(payloadData);
        this.chunk = payloadData;

        len = din.readInt();
        data = new byte[len];
        din.readFully(data);
        this.replica = (new String(data));

        inputData.close();
        din.close();

    }

    public byte[] getBytes() throws IOException {
        byte[] marshalledData;
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(outputStream));

        dout.writeInt(type);

        dout.writeBoolean(forward);

        dout.writeInt(sequenceNumber);

        dout.writeInt(filePath.getBytes().length);
        dout.write(filePath.getBytes());

        dout.writeInt(this.chunk.length);
        dout.write(this.chunk);

        byte[] bytes = replica.getBytes();
        dout.writeInt(bytes.length);
        dout.write(bytes);

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

    public String getReplica() {
        return replica;
    }

    public boolean checkForward() {
        return forward;
    }

}
