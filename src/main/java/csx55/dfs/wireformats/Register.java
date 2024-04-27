package csx55.dfs.wireformats;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class Register implements Event {
    private int type;

    private String ipAddress;
    private int port;
    private String hostName;

    public Register(int type, String ipAddress, int port, String hostName) {
        this.type = type;
        this.ipAddress = ipAddress;
        this.port = port;
        this.hostName = hostName;
    }

    public Register(byte[] marshalledData) throws IOException {

        ByteArrayInputStream inputData = new ByteArrayInputStream(marshalledData);

        DataInputStream din = new DataInputStream(new BufferedInputStream(inputData));

        this.type = din.readInt();

        int len = din.readInt();

        byte[] ipData = new byte[len];
        din.readFully(ipData, 0, len);

        this.ipAddress = new String(ipData);

        this.port = din.readInt();

        len = din.readInt();

        byte[] nameData = new byte[len];
        din.readFully(ipData, 0, len);

        this.hostName = new String(nameData);

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

        byte[] ipBytes = ipAddress.getBytes();
        dout.writeInt(ipBytes.length);
        dout.write(ipBytes);

        dout.writeInt(port);

        byte[] nameBytes = hostName.getBytes();
        dout.writeInt(nameBytes.length);
        dout.write(nameBytes);

        dout.flush();
        marshalledData = outputStream.toByteArray();

        outputStream.close();
        dout.close();
        return marshalledData;

    }

    public String getRegisterReadable() {
        return Integer.toString(this.type) + " " + this.getConnectionReadable();
    }

    public String getConnectionReadable() {
        return this.ipAddress + ":" + Integer.toString(this.port);
    }

    public String getIPAddress() {
        return ipAddress;
    }

    public int getHostPort() {
        return port;
    }

    public String getHostName() {
        return hostName;
    }

}