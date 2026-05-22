package Server.TCP;

import Server.Common.*;
import java.io.*;
import java.net.*;

// "Proxy" object for connections between Middleware and RMS
// One TCP_Proxy object per RM
public class TCP_Proxy {
    private String host;
    private int port;
    private Socket socket;
    private ObjectOutputStream out;
    private ObjectInputStream in;

    public TCP_Proxy(String host, int port) {
        this.host = host;
        this.port = port;
        try {
            this.socket = new Socket(host, port); // Example: socket = new Socket("tr-open-02", 3038);
            this.out = new ObjectOutputStream(socket.getOutputStream());
            this.in = new ObjectInputStream((socket.getInputStream()));
        } catch (IOException e) {
            e.printStackTrace();
        }
        
    }

    // Write TCPMessage (sent from a TCPClient) to output stream 
    // Sends message to an RM
    public synchronized TCPMessage send_TCP_request(TCPMessage request) {
        TCPMessage response = null;
        try {
            out.writeObject(request);
            out.flush();
            response = (TCPMessage) in.readObject();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            return null;
        }
        return response;
    }

    public void close() {
    try {
        if (socket != null) socket.close();
    } catch (IOException e) {
        e.printStackTrace();
    }
}
    
}
