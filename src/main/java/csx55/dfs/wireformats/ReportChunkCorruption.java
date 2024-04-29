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

public class ReportChunkCorruption implements Event {
    private int type;
    public String originChunkServer;
    public String chunkPath;
    public boolean isFixed;
    // public List<Integer> corruptedSlices;

    public ReportChunkCorruption(String originChunkServer, String chunkPath,
            boolean isFixed) {
        this.type = Protocol.REPORT_CHUNK_CORRUPTION;
        this.chunkPath = chunkPath;
        this.originChunkServer = originChunkServer;
        this.isFixed = isFixed;
        // this.corruptedSlices = corruptedSlices;
    }

    public ReportChunkCorruption(byte[] marshalledData) throws IOException {
        ByteArrayInputStream inputData = new ByteArrayInputStream(marshalledData);

        DataInputStream din = new DataInputStream(new BufferedInputStream(inputData));

        this.type = din.readInt();

        int len = din.readInt();
        byte[] data = new byte[len];
        din.readFully(data);
        this.chunkPath = new String(data);

        len = din.readInt();
        data = new byte[len];
        din.readFully(data);
        this.originChunkServer = new String(data);

        this.isFixed = din.readBoolean();

        // len = din.readInt();
        // this.corruptedSlices = new ArrayList<>(len);
        // for (int i = 0; i < len; i++) {
        // int index = din.readInt();
        // corruptedSlices.add(index);
        // }

        inputData.close();
        din.close();

    }

    public byte[] getBytes() throws IOException {
        byte[] marshalledData;
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(outputStream));

        dout.writeInt(type);

        dout.writeInt(chunkPath.getBytes().length);
        dout.write(chunkPath.getBytes());

        dout.writeInt(originChunkServer.getBytes().length);
        dout.write(originChunkServer.getBytes());

        dout.writeBoolean(isFixed);

        // dout.writeInt(corruptedSlices.size());
        // for (int slice : corruptedSlices) {
        // dout.writeInt(slice);
        // }

        dout.flush();
        marshalledData = outputStream.toByteArray();

        outputStream.close();
        dout.close();

        return marshalledData;
    }

    public int getType() {
        return type;
    }

}
