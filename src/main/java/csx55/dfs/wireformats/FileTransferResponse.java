package csx55.dfs.wireformats;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class FileTransferResponse implements Event {

    private int type;
    private byte status;
    private String info;

    public FileTransferResponse(byte status, String info) {
        this.type = Protocol.FILE_TRANSFER_RESPONSE;
        this.status = status;
        this.info = info;
    }

    public FileTransferResponse(byte[] marshalledData) throws IOException {
        ByteArrayInputStream inputData = new ByteArrayInputStream(marshalledData);
        DataInputStream din = new DataInputStream(new BufferedInputStream(inputData));

        this.type = din.readInt();
        this.status = din.readByte();
        int len = din.readInt();
        byte[] infoData = new byte[len];
        din.readFully(infoData, 0, len);
        this.info = new String(infoData);

        inputData.close();
        din.close();
    }

    public int getType() {
        return type;
    }

    public byte[] getBytes() throws IOException {
        ByteArrayOutputStream opStream = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(opStream));

        dout.writeInt(type);
        dout.writeByte(status);
        byte[] infoData = info.getBytes();
        dout.writeInt(infoData.length);
        dout.write(infoData);

        // making sure data from buffer is flushed
        dout.flush();
        byte[] marshalledData = opStream.toByteArray();

        opStream.close();
        dout.close();
        return marshalledData;
    }

    public String toString() {
        return info;
    }

}