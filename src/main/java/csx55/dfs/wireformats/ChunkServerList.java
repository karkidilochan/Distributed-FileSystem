package csx55.dfs.wireformats;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class ChunkServerList implements Event {
    private int type;

    /* string of ipAddress:port */
    private String hostAddressA;
    private String hostAddressB;
    private String hostAddressC;

    private String sourcePath;
    private String destinationPath;

    public ChunkServerList(String hostAddressA, String hostAddressB, String hostAddressC, String sourcePath,
            String destinationPath) {
        this.type = Protocol.CHUNK_SERVER_LIST;
        this.hostAddressA = hostAddressA;
        this.hostAddressB = hostAddressB;
        this.hostAddressC = hostAddressC;
        this.sourcePath = sourcePath;
        this.destinationPath = destinationPath;
    }

    public ChunkServerList(byte[] marshalledData) throws IOException {
        // creating input stream to read byte data sent over network connection
        ByteArrayInputStream inputData = new ByteArrayInputStream(marshalledData);

        // wrap internal bytes array with data input stream
        DataInputStream din = new DataInputStream(new BufferedInputStream(inputData));

        this.type = din.readInt();

        int len = din.readInt();
        byte[] stringData = new byte[len];
        din.readFully(stringData, 0, len);
        this.hostAddressA = new String(stringData);

        len = din.readInt();
        stringData = new byte[len];
        din.readFully(stringData, 0, len);
        this.hostAddressB = new String(stringData);

        len = din.readInt();
        stringData = new byte[len];
        din.readFully(stringData, 0, len);
        this.hostAddressC = new String(stringData);

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

        byte[] stringBytes = hostAddressA.getBytes();
        dout.writeInt(stringBytes.length);
        dout.write(stringBytes);

        stringBytes = hostAddressB.getBytes();
        dout.writeInt(stringBytes.length);
        dout.write(stringBytes);

        stringBytes = hostAddressC.getBytes();
        dout.writeInt(stringBytes.length);
        dout.write(stringBytes);

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

    public String getHostA() {
        return hostAddressA;
    }

    public String getHostB() {
        return hostAddressB;
    }

    public String getHostC() {
        return hostAddressC;
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