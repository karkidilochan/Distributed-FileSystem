package distributed.dfs.tcp;

import java.net.Socket;

import distributed.dfs.utils.Node;

import java.io.IOException;
import java.net.ServerSocket;;;

/**
 * Represents a server thread that accepts incoming TCP
 * connections.
 * Setting up server thread to accept new TCPConnetions on Registery and every
 * messaging nodes
 */
public class TCPServer implements Runnable {
    private Node node;
    private ServerSocket serverSocket;

    /**
     * Constructs a TCPServer object with the given node and server socket for the
     * server thread to run on.
     * 
     * @param node         The node associated with this server.
     * @param serverSocket The server socket for accepting connections.
     */
    public TCPServer(Node node, ServerSocket serverSocket) {
        this.node = node;
        this.serverSocket = serverSocket;
    }

    /**
     * Run the server thread to accept incoming TCP connections.
     */
    public void run() {
        while (serverSocket != null && !serverSocket.isClosed()) {
            try {
                // Accept incoming connection
                Socket incomingConnection = serverSocket.accept();

                // Create a new TCP connection for the incoming connection
                TCPConnection newConnection = new TCPConnection(node, incomingConnection);

                newConnection.start();
            } catch (IOException e) {
                System.out.println("Error accepting incoming connection: " + e.getMessage());
                e.printStackTrace();
                break;
            }
        }
    }
}
