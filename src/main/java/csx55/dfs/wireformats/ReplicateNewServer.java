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

public class ReplicateNewServer implements Event {
    private int type;

    /* string of ipAddress:port */
    public List<String> chunksList;
    public String targetIP;
    public int targetPort;

    public ReplicateNewServer(String targetIP, int targetPort, List<String> chunksList) {
        this.type = Protocol.REPLICATE_NEW_SERVER;
        this.chunksList = chunksList;
        this.targetIP = targetIP;
        this.targetPort = targetPort;
    }

    public ReplicateNewServer(byte[] marshalledData) throws IOException {
        // creating input stream to read byte data sent over network connection
        ByteArrayInputStream inputData = new ByteArrayInputStream(marshalledData);

        // wrap internal bytes array with data input stream
        DataInputStream din = new DataInputStream(new BufferedInputStream(inputData));

        this.type = din.readInt();

        int len;
        byte[] stringData;

        int numberOfChunks = din.readInt();
        this.chunksList = new ArrayList<String>();
        for (int i = 0; i < numberOfChunks; i++) {
            len = din.readInt();
            stringData = new byte[len];
            din.readFully(stringData);
            this.chunksList.add(new String(stringData));
        }

        len = din.readInt();
        stringData = new byte[len];
        din.readFully(stringData, 0, len);
        this.targetIP = new String(stringData);

        this.targetPort = din.readInt();

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

        dout.writeInt(chunksList.size());

        for (String data : chunksList) {
            byte[] bytes = data.getBytes();
            dout.writeInt(bytes.length);
            dout.write(bytes);
        }

        byte[] stringBytes;
        stringBytes = targetIP.getBytes();
        dout.writeInt(stringBytes.length);
        dout.write(stringBytes);

        dout.writeInt(targetPort);

        dout.flush();
        marshalledData = outputStream.toByteArray();

        outputStream.close();
        dout.close();
        return marshalledData;

    }

    public List<String> getList() {
        return chunksList;
    }

}