package Server.TCP;

import Server.Interface.*;
import Server.Common.*;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Vector;

public class TCPResourceManager extends ResourceManager {
    private static String s_rmiPrefix = "group_38_";
    private static int port = 3038;

    public TCPResourceManager(String name) {
        super(s_rmiPrefix + name);
    }

// Send TCPMessage response to Middleware, synchronized to avoid race condition
// when writing to output stream
private void sendResponse(ObjectOutputStream outputBuffer, TCPMessage response) throws IOException {
    synchronized (outputBuffer) {
        outputBuffer.writeObject(response);
        outputBuffer.flush();
    }
}

// Receives a TCP message from the Middleware
// Executes correct method and appends new TCPMessage object
// to output stream
private void process_message(TCPMessage message, ObjectOutputStream outputBuffer) {
    String cmd = message.getMethodName();
    CommandEnum cmd_enum = CommandEnum.valueOf(cmd);
    Vector<String> params = message.getParameters();

    try {
        int flightNum, numSeats, seatCost, customerID;
        String location;

        switch (cmd_enum) {
            case addFlight:
                flightNum = Integer.parseInt(params.get(0));
                numSeats = Integer.parseInt(params.get(1));
                seatCost = Integer.parseInt(params.get(2));
                sendResponse(outputBuffer, new TCPMessage(this.addFlight(flightNum, numSeats, seatCost)));
                break;

            case addCars:
                location = params.get(0);
                numSeats = Integer.parseInt(params.get(1));
                seatCost = Integer.parseInt(params.get(2));
                sendResponse(outputBuffer, new TCPMessage(this.addCars(location, numSeats, seatCost)));
                break;

            case addRooms:
                location = params.get(0);
                numSeats = Integer.parseInt(params.get(1));
                seatCost = Integer.parseInt(params.get(2));
                sendResponse(outputBuffer, new TCPMessage(this.addRooms(location, numSeats, seatCost)));
                break;

            case addCustomerID:
                customerID = Integer.parseInt(params.get(0));
                sendResponse(outputBuffer, new TCPMessage(this.newCustomer(customerID)));
                break;

            case deleteFlight:
                flightNum = Integer.parseInt(params.get(0));
                sendResponse(outputBuffer, new TCPMessage(this.deleteFlight(flightNum)));
                break;

            case deleteCars:
                location = params.get(0);
                sendResponse(outputBuffer, new TCPMessage(this.deleteCars(location)));
                break;

            case deleteRooms:
                location = params.get(0);
                sendResponse(outputBuffer, new TCPMessage(this.deleteRooms(location)));
                break;

            case deleteCustomer:
                customerID = Integer.parseInt(params.get(0));
                sendResponse(outputBuffer, new TCPMessage(this.deleteCustomer(customerID)));
                break;

            case queryFlight:
                flightNum = Integer.parseInt(params.get(0));
                sendResponse(outputBuffer, new TCPMessage(this.queryFlight(flightNum)));
                break;

            case queryCars:
                location = params.get(0);
                sendResponse(outputBuffer, new TCPMessage(this.queryCars(location)));
                break;

            case queryRooms:
                location = params.get(0);
                sendResponse(outputBuffer, new TCPMessage(this.queryRooms(location)));
                break;

            case queryCustomer:
                customerID = Integer.parseInt(params.get(0));
                sendResponse(outputBuffer, new TCPMessage(this.queryCustomerInfo(customerID)));
                break;

            case queryFlightPrice:
                flightNum = Integer.parseInt(params.get(0));
                sendResponse(outputBuffer, new TCPMessage(this.queryFlightPrice(flightNum)));
                break;

            case queryCarsPrice:
                location = params.get(0);
                sendResponse(outputBuffer, new TCPMessage(this.queryCarsPrice(location)));
                break;

            case queryRoomsPrice:
                location = params.get(0);
                sendResponse(outputBuffer, new TCPMessage(this.queryRoomsPrice(location)));
                break;

            case reserveFlight:
                customerID = Integer.parseInt(params.get(0));
                flightNum = Integer.parseInt(params.get(1));
                sendResponse(outputBuffer, new TCPMessage(this.reserveFlight(customerID, flightNum)));
                break;

            case reserveCar:
                customerID = Integer.parseInt(params.get(0));
                location = params.get(1);
                sendResponse(outputBuffer, new TCPMessage(this.reserveCar(customerID, location)));
                break;

            case reserveRoom:
                customerID = Integer.parseInt(params.get(0));
                location = params.get(1);
                sendResponse(outputBuffer, new TCPMessage(this.reserveRoom(customerID, location)));
                break;

            case cancelFlight:
                customerID = Integer.parseInt(params.get(0));
                flightNum = Integer.parseInt(params.get(1));
                sendResponse(outputBuffer, new TCPMessage(this.cancelFlight(customerID, flightNum)));
                break;

            case cancelCar:
                customerID = Integer.parseInt(params.get(0));
                location = params.get(1);
                sendResponse(outputBuffer, new TCPMessage(this.cancelCar(customerID, location)));
                break;

            case cancelRoom:
                customerID = Integer.parseInt(params.get(0));
                location = params.get(1);
                sendResponse(outputBuffer, new TCPMessage(this.cancelRoom(customerID, location)));
                break;

            case quit:
                System.out.println("Middleware requested quit. Closing connection.");
                return;

            default:
                throw new AssertionError("Command not found");
        }
    } catch (IOException e) {
        e.printStackTrace();
    }
}


    private void run_server() throws IOException {
        ServerSocket serverSocket = new ServerSocket(TCPResourceManager.port);
        System.out.println("Server started on port " + port + " for RM: " + m_name);

        // Accept a single connection from the Middleware
        Socket clientSocket = serverSocket.accept();
        System.out.println("Accepted connection request from Middleware");

        ObjectOutputStream out = new ObjectOutputStream(clientSocket.getOutputStream());
        ObjectInputStream in = new ObjectInputStream(clientSocket.getInputStream());

        try {
            while (true) {
                TCPMessage request = null;
                try {
                    request = (TCPMessage) in.readObject();
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                    continue;
                }
                System.out.println("Processing " + request.getMethodName());
                final TCPMessage request_final = request;
                new Thread(() -> { // Handle incoming request to RM concurrently
                    process_message(request_final, out);
                }).start();
            }
        } catch (IOException e) {
            System.out.println("Client disconnected.");
        } finally {
            try {
                in.close();
                out.close();
                clientSocket.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        String rm_name = "TCP_RM"; // Default value
        if (args.length > 0) {
            rm_name = args[0];
        }
        else {
            System.err.println("Resource manager name must be provided");
            System.exit(1);
        }
        TCPResourceManager rm = new TCPResourceManager(rm_name);
        try {
            rm.run_server();
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("Running " + s_rmiPrefix + rm_name + " on port " + TCPResourceManager.port);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                try {
                    System.out.println("resource manager unbound");
                } catch (Exception e) {
                    System.err.println((char) 27 + "[31;1mServer exception: " + (char) 27 + "[0mUncaught exception");
                    e.printStackTrace();
                }
            }
        });
    }
}