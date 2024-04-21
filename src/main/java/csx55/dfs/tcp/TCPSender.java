package csx55.dfs.tcp;

import java.net.Socket;
import java.util.concurrent.LinkedBlockingQueue;
import java.io.IOException;
import java.io.DataOutputStream;

/**
 * TCPSender class represents a sender thread for sending data over a TCP
 * connection.
 */
public class TCPSender implements Runnable {

    // LinkedBlockingQueue provides thread-safe blocking operations
    private LinkedBlockingQueue<byte[]> queue;

    protected DataOutputStream dout;

    /**
     * Construct a TCPSender object with the given socket.
     * 
     * @param socket The socket for the connection.
     */
    public TCPSender(Socket socket) throws IOException {
        final int queueSize = 1000;
        this.queue = new LinkedBlockingQueue<>(queueSize);
        this.dout = new DataOutputStream(socket.getOutputStream());
    }

    /**
     * Adds data to the sender's queue for sending.
     * 
     * @param data The data to be sent.
     */
    public void sendData(final byte[] data) throws InterruptedException {
        this.queue.put(data);
    }

    /**
     * Continuously sends data from the queue to the receiver through the
     * connection.
     * send data to receiver through the connection thread using data output stream
     * keeps running and takes whatever is in the queue, writes the length and the
     * data
     */
    public void run() {
        while (true) {
            try {
                // method will block on this until there is data on the queue
                byte[] data = queue.take();
                int len = data.length;

                // First write the length and the data to the output stream
                dout.writeInt(len);
                dout.write(data, 0, len);
                dout.flush();
                // no need to close the data output stream
            } catch (IOException | InterruptedException e) {
                System.out.println("Error at TCPSender Thread:" + e.getMessage());
                e.printStackTrace();
            }
        }
    }

}
