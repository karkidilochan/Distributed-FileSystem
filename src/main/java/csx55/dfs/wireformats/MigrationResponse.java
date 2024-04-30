package csx55.dfs.wireformats;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class MigrationResponse implements Event {

    private int type;
    private byte status;
    private String info;
    private int sequenceNumber;

    public MigrationResponse(byte status, String info, int sequenceNumber) {
        this.type = Protocol.MIGRATE_CHUNK_RESPONSE;
        this.status = status;
        this.info = info;
        this.sequenceNumber = sequenceNumber;
    }

    public MigrationResponse(byte[] marshalledData) throws IOException {
        ByteArrayInputStream inputData = new ByteArrayInputStream(marshalledData);
        DataInputStream din = new DataInputStream(new BufferedInputStream(inputData));

        this.type = din.readInt();
        this.status = din.readByte();
        this.sequenceNumber = din.readInt();

        int len = din.readInt();
        byte[] infoData = new byte[len];
        din.readFully(infoData, 0, len);
        this.info = new String(infoData);

        inputData.close();
        din.close();
    }

    public int getType() {
        return type;
    }

    public byte[] getBytes() throws IOException {
        ByteArrayOutputStream opStream = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(opStream));

        dout.writeInt(type);
        dout.writeByte(status);
        dout.writeInt(sequenceNumber);

        byte[] infoData = info.getBytes();
        dout.writeInt(infoData.length);
        dout.write(infoData);

        // making sure data from buffer is flushed
        dout.flush();
        byte[] marshalledData = opStream.toByteArray();

        opStream.close();
        dout.close();
        return marshalledData;
    }

    public String toString() {
        return info;
    }

    public int getSequenceNumber() {
        return sequenceNumber;
    }

}