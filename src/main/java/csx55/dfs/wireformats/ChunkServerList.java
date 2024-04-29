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

public class ChunkServerList implements Event {
    private int type;

    /* string of ipAddress:port */

    private List<String> hostAddresses;
    private String destinationPath;
    private int sequenceNumber;

    public ChunkServerList(List<String> hostAddresses,
            String destinationPath, int sequenceNumber) {
        this.type = Protocol.CHUNK_SERVER_LIST;
        this.sequenceNumber = sequenceNumber;
        this.hostAddresses = hostAddresses;
        this.destinationPath = destinationPath;
    }

    public ChunkServerList(byte[] marshalledData) throws IOException {
        // creating input stream to read byte data sent over network connection
        ByteArrayInputStream inputData = new ByteArrayInputStream(marshalledData);

        // wrap internal bytes array with data input stream
        DataInputStream din = new DataInputStream(new BufferedInputStream(inputData));

        this.type = din.readInt();

        this.sequenceNumber = din.readInt();

        int len;
        byte[] stringData;

        this.hostAddresses = new ArrayList<String>();
        for (int i = 0; i < 3; i++) {
            len = din.readInt();
            stringData = new byte[len];
            din.readFully(stringData);
            this.hostAddresses.add(new String(stringData));
        }

        len = din.readInt();
        stringData = new byte[len];
        din.readFully(stringData, 0, len);
        this.destinationPath = new String(stringData);

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

        dout.writeInt(sequenceNumber);

        for (String server : hostAddresses) {
            byte[] bytes = server.getBytes();
            dout.writeInt(bytes.length);
            dout.write(bytes);
        }

        byte[] stringBytes;
        stringBytes = destinationPath.getBytes();
        dout.writeInt(stringBytes.length);
        dout.write(stringBytes);

        dout.flush();
        marshalledData = outputStream.toByteArray();

        outputStream.close();
        dout.close();
        return marshalledData;

    }

    public List<String> getList() {
        return hostAddresses;
    }

    public String getDestinationPath() {
        return destinationPath;
    }

    public String getIPAddress(String hostString) {
        return hostString.split(":")[0];
    }

    public int getPort(String hostString) {
        return Integer.valueOf(hostString.split(":")[1]);
    }

    public int getSequence() {
        return sequenceNumber;
    }

}