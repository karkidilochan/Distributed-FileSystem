package csx55.dfs.wireformats;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class FileTransfer implements Event {

    private String fileName;
    private int type;
    private byte[] payload;

    public FileTransfer(String fileName, byte[] payload) {
        this.type = Protocol.FILE_TRANSFER;
        this.fileName = fileName;
        this.payload = payload;
    }

    public FileTransfer(byte[] marshalledData) throws IOException {
        ByteArrayInputStream inputData = new ByteArrayInputStream(marshalledData);

        DataInputStream din = new DataInputStream(new BufferedInputStream(inputData));

        this.type = din.readInt();

        int len = din.readInt();
        byte[] data = new byte[len];
        din.readFully(data);
        this.fileName = new String(data);

        len = din.readInt();
        byte[] payloadData = new byte[len];
        din.readFully(payloadData);
        this.payload = payloadData;

        inputData.close();
        din.close();

    }

    public byte[] getBytes() throws IOException {
        byte[] marshalledData;
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(outputStream));

        dout.writeInt(type);

        dout.writeInt(fileName.getBytes().length);
        dout.write(fileName.getBytes());

        dout.writeInt(this.payload.length);
        dout.write(this.payload);

        dout.flush();
        marshalledData = outputStream.toByteArray();

        outputStream.close();
        dout.close();

        return marshalledData;
    }

    public byte[] getFilePayload() {
        return payload;
    }

    public String getFileName() {
        return fileName;
    }

    public int getType() {
        return type;
    }

}
