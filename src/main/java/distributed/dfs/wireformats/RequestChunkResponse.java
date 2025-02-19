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

public class RequestChunkResponse implements Event {
    /* filepath is the destination path */
    private String downloadPath;
    private int totalSize;
    private int type;
    private byte[] chunk;
    private int sequenceNumber;

    public RequestChunkResponse(String downloadPath, int sequenceNumber, byte[] chunk, int totalSize) {
        this.type = Protocol.REQUEST_CHUNK_RESPONSE;
        this.downloadPath = downloadPath;
        this.sequenceNumber = sequenceNumber;
        this.chunk = chunk;
        this.totalSize = totalSize;
    }

    public RequestChunkResponse(byte[] marshalledData) throws IOException {
        ByteArrayInputStream inputData = new ByteArrayInputStream(marshalledData);

        DataInputStream din = new DataInputStream(new BufferedInputStream(inputData));

        this.type = din.readInt();

        this.sequenceNumber = din.readInt();

        this.totalSize = din.readInt();

        int len = din.readInt();
        byte[] data = new byte[len];
        din.readFully(data);
        this.downloadPath = new String(data);

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

        dout.writeInt(sequenceNumber);

        dout.writeInt(totalSize);

        dout.writeInt(downloadPath.getBytes().length);
        dout.write(downloadPath.getBytes());

        dout.writeInt(this.chunk.length);
        dout.write(this.chunk);

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
        return downloadPath;
    }

    public int getType() {
        return type;
    }

    public int getSequence() {
        return sequenceNumber;
    }

    public int getTotalSize() {
        return totalSize;
    }

}
