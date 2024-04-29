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

public class ChunkCorrection implements Event {
    /* filepath is the destination path */
    public String filePath;
    private int type;
    public String ipAddress;
    public int port;
    public byte[] chunk;

    public ChunkCorrection(String filePath, byte[] chunk) {
        this.type = Protocol.ERROR_CORRECTION;
        this.filePath = filePath;

        this.chunk = chunk;
    }

    public ChunkCorrection(byte[] marshalledData) throws IOException {
        ByteArrayInputStream inputData = new ByteArrayInputStream(marshalledData);

        DataInputStream din = new DataInputStream(new BufferedInputStream(inputData));

        this.type = din.readInt();

        int len = din.readInt();
        byte[] data = new byte[len];
        din.readFully(data);
        this.filePath = new String(data);

        len = din.readInt();
        byte[] payloadData = new byte[len];
        din.readFully(payloadData);
        this.chunk = payloadData;

        inputData.close();
        din.close();

    }

    public byte[] getBytes() throws IOException {
        byte[] marshalledData;
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(outputStream));

        dout.writeInt(type);

        dout.writeInt(filePath.getBytes().length);
        dout.write(filePath.getBytes());

        dout.writeInt(this.chunk.length);
        dout.write(this.chunk);

        dout.flush();
        marshalledData = outputStream.toByteArray();

        outputStream.close();
        dout.close();

        return marshalledData;
    }

    public String getFilePath() {
        return filePath;
    }

    public int getType() {
        return type;
    }

}
