package csx55.dfs.wireformats;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

public class MigrateChunk implements Event {
    /* filepath is the destination path */
    public String chunkPath;
    private int type;
    public byte[] chunk;
    public int sequenceNumber;
    public String fileName;

    public MigrateChunk(int sequenceNumber, String chunkPath, byte[] chunk, String fileName) {
        this.type = Protocol.MIGRATE_CHUNK;
        this.chunkPath = chunkPath;
        this.chunk = chunk;
        this.sequenceNumber = sequenceNumber;
        this.fileName = fileName;
    }

    public MigrateChunk(byte[] marshalledData) throws IOException {
        ByteArrayInputStream inputData = new ByteArrayInputStream(marshalledData);

        DataInputStream din = new DataInputStream(new BufferedInputStream(inputData));

        this.type = din.readInt();

        this.sequenceNumber = din.readInt();

        int len = din.readInt();
        byte[] data = new byte[len];
        din.readFully(data);
        this.chunkPath = new String(data);

        len = din.readInt();
        byte[] payloadData = new byte[len];
        din.readFully(payloadData);
        this.chunk = payloadData;

        len = din.readInt();
        data = new byte[len];
        din.readFully(data);
        this.fileName = new String(data);

        inputData.close();
        din.close();

    }

    public byte[] getBytes() throws IOException {
        byte[] marshalledData;
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(outputStream));

        dout.writeInt(type);

        dout.writeInt(sequenceNumber);

        dout.writeInt(chunkPath.getBytes().length);
        dout.write(chunkPath.getBytes());

        dout.writeInt(this.chunk.length);
        dout.write(this.chunk);

        dout.writeInt(fileName.getBytes().length);
        dout.write(fileName.getBytes());

        dout.flush();
        marshalledData = outputStream.toByteArray();

        outputStream.close();
        dout.close();

        return marshalledData;
    }

    public byte[] getChunk() {
        return chunk;
    }

    public int getType() {
        return type;
    }

}
