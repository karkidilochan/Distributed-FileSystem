package csx55.dfs.tcp;

import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.TimeUnit;

import csx55.dfs.utils.Node;;

/**
 * Represents a TCP connection between nodes for sending and receiving
 * messages.
 */
public class TCPConnection {
    private Socket socket;
    private TCPSender sender;
    private TCPReceiver receiver;

    /**
     * Construct a new TCPConnection for the given node and socket.
     * 
     * @param node   The node associated with this TCP connection.
     * @param socket The socket for the connection.
     */
    public TCPConnection(Node node, Socket socket) throws IOException {
        this.socket = socket;
        this.sender = new TCPSender(this.socket);
        this.receiver = new TCPReceiver(node, this.socket, this);
    }

    /**
     * Retrieves the socket associated with this TCP connection.
     * 
     * @return The socket object.
     */
    public Socket getSocket() {
        return this.socket;
    }

    /**
     * Retrieves the sender thread associated with this TCP connection.
     * 
     * @return The TCPSender object.
     */
    public TCPSender getTCPSenderThread() {
        return this.sender;
    }

    /**
     * Starts the sender and receiver threads for sending and receiving messages.
     */
    public void start() {
        (new Thread(this.receiver)).start();
        (new Thread(this.sender)).start();
    }

    /**
     * Closes the TCP connection, including sender and receiver threads,
     * with a 5-second sleep to ensure all remaining messages are sent.
     * 
     * @throws IOException          If an I/O error occurs.
     * @throws InterruptedException If the sleep is interrupted.
     */
    public void close() throws IOException, InterruptedException {
        TimeUnit.SECONDS.sleep(5);
        this.socket.close();
        this.sender.dout.close();
        this.receiver.din.close();
    }

}
