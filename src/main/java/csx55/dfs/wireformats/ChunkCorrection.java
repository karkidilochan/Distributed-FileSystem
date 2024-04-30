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
    public List<Integer> corruptedSliceIndexes;
    public List<byte[]> correctSlices;

    public ChunkCorrection(String filePath, List<Integer> corruptedSliceIndexes, List<byte[]> correctSlices) {
        this.type = Protocol.CHUNK_CORRECTION;
        this.filePath = filePath;
        this.corruptedSliceIndexes = corruptedSliceIndexes;
        this.correctSlices = correctSlices;
    }

    public ChunkCorrection(byte[] marshalledData) throws IOException {
        ByteArrayInputStream inputData = new ByteArrayInputStream(marshalledData);

        DataInputStream din = new DataInputStream(new BufferedInputStream(inputData));

        this.type = din.readInt();

        int len = din.readInt();
        byte[] data = new byte[len];
        din.readFully(data);
        this.filePath = new String(data);

        int listLen = din.readInt();
        this.correctSlices = new ArrayList<>(listLen);
        for (int i = 0; i < listLen; i++) {
            len = din.readInt();
            byte[] payloadData = new byte[len];
            din.readFully(payloadData);
            this.correctSlices.add(payloadData);
        }

        len = din.readInt();
        this.corruptedSliceIndexes = new ArrayList<>(len);
        for (int i = 0; i < len; i++) {
            int index = din.readInt();
            this.corruptedSliceIndexes.add(index);
        }

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

        dout.writeInt(this.correctSlices.size());
        for (byte[] slice : correctSlices) {
            dout.writeInt(slice.length);
            dout.write(slice);
        }

        dout.writeInt(corruptedSliceIndexes.size());
        for (int slice : corruptedSliceIndexes) {
            dout.writeInt(slice);
        }

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
