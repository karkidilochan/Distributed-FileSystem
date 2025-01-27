package distributed.dfs.tcp;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket;

import distributed.dfs.utils.Node;
import distributed.dfs.wireformats.Event;
import distributed.dfs.wireformats.WireFormatGenerator;

/**
 * TCPReceiver class represents a receiver thread for handling incoming messages
 * over a TCP connection.
 */
public class TCPReceiver implements Runnable {

    private Socket socket;
    private Node node;
    private TCPConnection connection;

    protected DataInputStream din;

    /**
     * Constructs a TCPReceiver object with the given node, socket, and connection.
     * 
     * @param node       The node associated with this receiver.
     * @param socket     The socket for the connection.
     * @param connection The TCP connection.
     */
    public TCPReceiver(Node node, Socket socket, TCPConnection connection) throws IOException {
        this.node = node;
        this.socket = socket;
        this.connection = connection;
        this.din = new DataInputStream(socket.getInputStream());
    }

    /**
     * Runs the receiver thread to continuously read incoming data from the socket
     * input stream.
     */
    public void run() {
        while (socket != null) {
            try {
                // first read the length of data
                int len = din.readInt();

                byte[] data = new byte[len];
                din.readFully(data, 0, len);

                // create a message generator from the read data
                WireFormatGenerator messageGenerator = WireFormatGenerator.getInstance();
                Event event = messageGenerator.createMessage(data);

                node.handleIncomingEvent(event, connection);

            } catch (IOException e) {
                // TODO: handle exception
                // in case of error, stop listening to the socket
                System.out.println("Connection Closed." + e.getMessage());
                e.printStackTrace();
                break;
            }
        }
    }
}
