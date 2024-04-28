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
    private int size;
    private String sourcePath;
    private String destinationPath;

    public ChunkServerList(List<String> hostAddresses, String sourcePath,
            String destinationPath, int size) {
        this.type = Protocol.CHUNK_SERVER_LIST;
        this.size = size;
        this.hostAddresses = hostAddresses;
        this.sourcePath = sourcePath;
        this.destinationPath = destinationPath;
    }

    public ChunkServerList(byte[] marshalledData) throws IOException {
        // creating input stream to read byte data sent over network connection
        ByteArrayInputStream inputData = new ByteArrayInputStream(marshalledData);

        // wrap internal bytes array with data input stream
        DataInputStream din = new DataInputStream(new BufferedInputStream(inputData));

        this.type = din.readInt();

        this.size = din.readInt();

        int len;
        byte[] stringData;

        this.hostAddresses = new ArrayList<String>(this.size);
        for (int i = 0; i < this.size; i++) {
            len = din.readInt();
            stringData = new byte[len];
            din.readFully(stringData);
            this.hostAddresses.add(new String(stringData));
        }

        len = din.readInt();
        stringData = new byte[len];
        din.readFully(stringData, 0, len);
        this.sourcePath = new String(stringData);

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

        dout.writeInt(size);

        for (String server : hostAddresses) {
            byte[] bytes = server.getBytes();
            dout.writeInt(bytes.length);
            dout.write(bytes);
        }

        byte[] stringBytes;

        stringBytes = sourcePath.getBytes();
        dout.writeInt(stringBytes.length);
        dout.write(stringBytes);

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

    public String getSourcePath() {
        return sourcePath;
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

}