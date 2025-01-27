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

public class FetchChunksListResponse implements Event {
    private int type;

    /* string of ipAddress:port */
    public int numberOfChunks;
    public List<String> chunksList;
    public String downloadPath;
    public String clusterPath;
    public List<String> chunkServerList;

    public FetchChunksListResponse(int numberOfChunks, List<String> chunksList, List<String> chunkServerList,
            String clusterPath,
            String downloadPath) {
        this.type = Protocol.FETCH_CHUNKS_RESPONSE;
        this.numberOfChunks = numberOfChunks;
        this.chunksList = chunksList;
        this.chunkServerList = chunkServerList;
        this.clusterPath = clusterPath;
        this.downloadPath = downloadPath;
    }

    public FetchChunksListResponse(byte[] marshalledData) throws IOException {
        // creating input stream to read byte data sent over network connection
        ByteArrayInputStream inputData = new ByteArrayInputStream(marshalledData);

        // wrap internal bytes array with data input stream
        DataInputStream din = new DataInputStream(new BufferedInputStream(inputData));

        this.type = din.readInt();

        this.numberOfChunks = din.readInt();

        int len;
        byte[] stringData;

        this.chunksList = new ArrayList<String>();
        for (int i = 0; i < numberOfChunks; i++) {
            len = din.readInt();
            stringData = new byte[len];
            din.readFully(stringData);
            this.chunksList.add(new String(stringData));
        }

        this.chunkServerList = new ArrayList<String>();
        for (int i = 0; i < numberOfChunks; i++) {
            len = din.readInt();
            stringData = new byte[len];
            din.readFully(stringData);
            this.chunkServerList.add(new String(stringData));
        }

        len = din.readInt();
        stringData = new byte[len];
        din.readFully(stringData, 0, len);
        this.clusterPath = new String(stringData);

        len = din.readInt();
        stringData = new byte[len];
        din.readFully(stringData, 0, len);
        this.downloadPath = new String(stringData);

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

        dout.writeInt(numberOfChunks);

        for (String data : chunksList) {
            byte[] bytes = data.getBytes();
            dout.writeInt(bytes.length);
            dout.write(bytes);
        }

        for (String data : chunkServerList) {
            byte[] bytes = data.getBytes();
            dout.writeInt(bytes.length);
            dout.write(bytes);
        }

        byte[] stringBytes;
        stringBytes = clusterPath.getBytes();
        dout.writeInt(stringBytes.length);
        dout.write(stringBytes);

        stringBytes = downloadPath.getBytes();
        dout.writeInt(stringBytes.length);
        dout.write(stringBytes);

        dout.flush();
        marshalledData = outputStream.toByteArray();

        outputStream.close();
        dout.close();
        return marshalledData;

    }

    public List<String> getList() {
        return chunksList;
    }

    public String getDestinationPath() {
        return downloadPath;
    }

    public String getSourcePath() {
        return downloadPath;
    }

    public int getChunksCount() {
        return numberOfChunks;
    }

}