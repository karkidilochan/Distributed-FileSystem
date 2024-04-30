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

public class ErrorCorrection implements Event {
    /* filepath is the destination path */
    public String filePath;
    private int type;
    public String ipAddress;
    public int port;

    public String requestingClientIP;
    public int requestingClientPort;
    public String clusterPath;
    public String downloadPath;
    public int sequenceNumber;
    public int totalSize;
    public List<Integer> corruptedSlices;

    public ErrorCorrection(String filePath, String ipAddress, int port, String clusterPath, String downloadPath,
            int sequenceNumber, int totalSize, String requestingClientIP, int requestingClientPort,
            List<Integer> corruptedSlices) {
        this.type = Protocol.ERROR_CORRECTION;
        this.filePath = filePath;
        this.ipAddress = ipAddress;
        this.port = port;

        this.requestingClientIP = requestingClientIP;
        this.requestingClientPort = requestingClientPort;
        this.clusterPath = clusterPath;
        this.downloadPath = downloadPath;
        this.sequenceNumber = sequenceNumber;
        this.totalSize = totalSize;
        this.corruptedSlices = corruptedSlices;
    }

    public ErrorCorrection(byte[] marshalledData) throws IOException {
        ByteArrayInputStream inputData = new ByteArrayInputStream(marshalledData);

        DataInputStream din = new DataInputStream(new BufferedInputStream(inputData));

        this.type = din.readInt();

        int len = din.readInt();
        byte[] data = new byte[len];
        din.readFully(data);
        this.filePath = new String(data);

        len = din.readInt();
        data = new byte[len];
        din.readFully(data);
        this.ipAddress = new String(data);

        this.port = din.readInt();

        this.sequenceNumber = din.readInt();
        this.totalSize = din.readInt();

        len = din.readInt();
        data = new byte[len];
        din.readFully(data);
        this.clusterPath = new String(data);

        len = din.readInt();
        data = new byte[len];
        din.readFully(data);
        this.downloadPath = new String(data);

        len = din.readInt();
        data = new byte[len];
        din.readFully(data);
        this.requestingClientIP = new String(data);

        this.requestingClientPort = din.readInt();

        len = din.readInt();
        this.corruptedSlices = new ArrayList<>(len);
        for (int i = 0; i < len; i++) {
            int index = din.readInt();
            this.corruptedSlices.add(index);
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

        dout.writeInt(ipAddress.getBytes().length);
        dout.write(ipAddress.getBytes());

        dout.writeInt(port);

        dout.writeInt(sequenceNumber);
        dout.writeInt(totalSize);

        dout.writeInt(clusterPath.getBytes().length);
        dout.write(clusterPath.getBytes());

        dout.writeInt(downloadPath.getBytes().length);
        dout.write(downloadPath.getBytes());

        dout.writeInt(requestingClientIP.getBytes().length);
        dout.write(requestingClientIP.getBytes());

        dout.writeInt(requestingClientPort);

        dout.writeInt(corruptedSlices.size());
        for (int slice : corruptedSlices) {
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
