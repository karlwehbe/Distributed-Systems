package Client;

import java.io.IOException;
import Server.Common.TCPMessage;
import java.net.*;
import java.io.*;
import java.util.Vector;

public class TCP_Client extends Client {
    private static String middlewareHost = "tr-open-01";
    private static int middlewarePort = 3038;
    private Socket clientSocket;
    private ObjectOutputStream out;
    private ObjectInputStream in;

    public TCP_Client(String m_Hostname, int m_Port) {
        middlewareHost = m_Hostname;
        middlewarePort = m_Port;
    }

    @Override
    public void connectServer() {
        try {
            this.clientSocket = new Socket(middlewareHost, middlewarePort);
            this.out = new ObjectOutputStream(clientSocket.getOutputStream());
            this.in = new ObjectInputStream(clientSocket.getInputStream());
            System.out.println("Connected to server " + middlewareHost + ":" + middlewarePort);
        } catch (IOException e) {
            System.err.println("Failed to connect: " + e.getMessage());
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Command cmd, Vector<String> arguments) {
        try {
            switch (cmd) {
                case AddFlight: {
                    checkArgumentsCount(4, arguments.size());
                    System.out.println("Adding a new flight ");
                    System.out.println("-Flight Number: " + arguments.elementAt(1));
                    System.out.println("-Flight Seats: " + arguments.elementAt(2));
                    System.out.println("-Flight Price: " + arguments.elementAt(3));

                    Vector<String> params = new Vector<>();
                    params.add(arguments.elementAt(1)); // flight number
                    params.add(arguments.elementAt(2)); // seats
                    params.add(arguments.elementAt(3)); // price
                    TCPMessage request = new TCPMessage("addFlight", params);
                    synchronized (out) {
                        out.writeObject(request);
                        out.flush();
                    }
                    TCPMessage response = (TCPMessage) in.readObject();
                    System.out.println(response.getBoolResponse() ? "Flight added" : "Flight could not be added");
                    break;
                }

                case AddCars: {
                    checkArgumentsCount(4, arguments.size());
                    System.out.println("Adding new cars");
                    System.out.println("-Car Location: " + arguments.elementAt(1));
                    System.out.println("-Number of Cars: " + arguments.elementAt(2));
                    System.out.println("-Car Price: " + arguments.elementAt(3));

                    Vector<String> params = new Vector<>();
                    params.add(arguments.elementAt(1)); // location
                    params.add(arguments.elementAt(2)); // count
                    params.add(arguments.elementAt(3)); // price
                    TCPMessage request = new TCPMessage("addCars", params);
                    synchronized (out) {
                        out.writeObject(request);
                        out.flush();
                    }
                    TCPMessage response = (TCPMessage) in.readObject();
                    System.out.println(response.getBoolResponse() ? "Cars added" : "Cars could not be added");
                    break;
                }   

                case AddRooms: {
                    checkArgumentsCount(4, arguments.size());
                    System.out.println("Adding new rooms");
                    System.out.println("-Room Location: " + arguments.elementAt(1));
                    System.out.println("-Number of Rooms: " + arguments.elementAt(2));
                    System.out.println("-Room Price: " + arguments.elementAt(3));

                    Vector<String> params = new Vector<>();
                    params.add(arguments.elementAt(1)); // location
                    params.add(arguments.elementAt(2)); // count
                    params.add(arguments.elementAt(3)); // price
                    TCPMessage request = new TCPMessage("addRooms", params);
                    synchronized (out) {
                        out.writeObject(request);
                        out.flush();
                    }
                    TCPMessage response = (TCPMessage) in.readObject();
                    System.out.println(response.getBoolResponse() ? "Rooms added" : "Rooms could not be added");
                    break;
                }

                case AddCustomer: {
                    checkArgumentsCount(1, arguments.size());
                    System.out.println("Adding a new customer");

                    Vector<String> params = new Vector<>(); // no parameters for addCustomer
                    TCPMessage request = new TCPMessage("addCustomer", params);
                    synchronized (out) {
                        out.writeObject(request);
                        out.flush();
                    }
                    TCPMessage response = (TCPMessage) in.readObject();
                    System.out.println(response.getIntResponse() != 0 ? "Add customer ID: " + response.getIntResponse() : "Customer could not be added");
                    break;
                }

                case AddCustomerID: {
                    checkArgumentsCount(2, arguments.size());
                    System.out.println("Adding a new customer");
                    System.out.println("-Customer ID: " + arguments.elementAt(1));

                    Vector<String> params = new Vector<>();
                    params.add(arguments.elementAt(1)); // customer ID
                    TCPMessage request = new TCPMessage("addCustomerID", params);
                    synchronized (out) {
                        out.writeObject(request);
                        out.flush();
                    }
                    TCPMessage response = (TCPMessage) in.readObject();
                    System.out.println(response.getBoolResponse() ? "Add customer ID: " + arguments.elementAt(1) : "Customer could not be added");
                    break;
                }

                case DeleteFlight: {
                    checkArgumentsCount(2, arguments.size());
                    System.out.println("Deleting a flight");
                    System.out.println("-Flight Number: " + arguments.elementAt(1));

                    Vector<String> params = new Vector<>();
                    params.add(arguments.elementAt(1)); // flight number
                    TCPMessage request = new TCPMessage("deleteFlight", params);
                    synchronized (out) {
                        out.writeObject(request);
                        out.flush();
                    }
                    TCPMessage response = (TCPMessage) in.readObject();
                    System.out.println(response.getBoolResponse() ? "Flight Deleted" : "Flight could not be deleted");
                    break;
                }

                case DeleteCars: {
                    checkArgumentsCount(2, arguments.size());
                    System.out.println("Deleting all cars at a particular location");
                    System.out.println("-Car Location: " + arguments.elementAt(1));

                    Vector<String> params = new Vector<>();
                    params.add(arguments.elementAt(1)); // location
                    TCPMessage request = new TCPMessage("deleteCars", params);
                    synchronized (out) {
                        out.writeObject(request);
                        out.flush();
                    }
                    TCPMessage response = (TCPMessage) in.readObject();
                    System.out.println(response.getBoolResponse() ? "Cars Deleted" : "Cars could not be deleted");
                    break;
                }

                case DeleteRooms: {
                    checkArgumentsCount(2, arguments.size());
                    System.out.println("Deleting all rooms at a particular location");
                    System.out.println("-Room Location: " + arguments.elementAt(1));

                    Vector<String> params = new Vector<>();
                    params.add(arguments.elementAt(1)); // location
                    TCPMessage request = new TCPMessage("deleteRooms", params);
                    synchronized (out) {
                        out.writeObject(request);
                        out.flush();
                    }
                    TCPMessage response = (TCPMessage) in.readObject();
                    System.out.println(response.getBoolResponse() ? "Rooms Deleted" : "Rooms could not be deleted");
                    break;
                }

                case DeleteCustomer: {
                    checkArgumentsCount(2, arguments.size());
                    System.out.println("Deleting a customer from the database");
                    System.out.println("-Customer ID: " + arguments.elementAt(1));

                    Vector<String> params = new Vector<>();
                    params.add(arguments.elementAt(1)); // customer ID
                    TCPMessage request = new TCPMessage("deleteCustomer", params);
                    synchronized (out) {
                        out.writeObject(request);
                        out.flush();
                    }
                    TCPMessage response = (TCPMessage) in.readObject();
                    System.out.println(response.getBoolResponse() ? "Customer Deleted" : "Customer could not be deleted");
                    break;
                }

                case QueryFlight: {
                    checkArgumentsCount(2, arguments.size());
                    System.out.println("Querying a flight");
                    System.out.println("-Flight Number: " + arguments.elementAt(1));

                    Vector<String> params = new Vector<>();
                    params.add(arguments.elementAt(1)); // flight number
                    TCPMessage request = new TCPMessage("queryFlight", params);
                    synchronized (out) {
                        out.writeObject(request);
                        out.flush();
                    }
                    TCPMessage response = (TCPMessage) in.readObject();
                    System.out.println("Number of seats available: " + response.getIntResponse());
                    break;
                }

                case QueryCars: {
                    checkArgumentsCount(2, arguments.size());
                    System.out.println("Querying cars location");
                    System.out.println("-Car Location: " + arguments.elementAt(1));

                    Vector<String> params = new Vector<>();
                    params.add(arguments.elementAt(1)); // location
                    TCPMessage request = new TCPMessage("queryCars", params);
                    synchronized (out) {
                        out.writeObject(request);
                        out.flush();
                    }
                    TCPMessage response = (TCPMessage) in.readObject();
                    System.out.println("Number of cars at this location: " + response.getIntResponse());
                    break;
                }

                case QueryRooms: {
                    checkArgumentsCount(2, arguments.size());
                    System.out.println("Querying rooms location");
                    System.out.println("-Room Location: " + arguments.elementAt(1));

                    Vector<String> params = new Vector<>();
                    params.add(arguments.elementAt(1)); // location
                    TCPMessage request = new TCPMessage("queryRooms", params);
                    synchronized (out) {
                        out.writeObject(request);
                        out.flush();
                    }
                    TCPMessage response = (TCPMessage) in.readObject();
                    System.out.println("Number of rooms at this location: " + response.getIntResponse());
                    break;
                }

                case QueryCustomer: {
                    checkArgumentsCount(2, arguments.size());
                    System.out.println("Querying customer information");
                    System.out.println("-Customer ID: " + arguments.elementAt(1));

                    Vector<String> params = new Vector<>();
                    params.add(arguments.elementAt(1)); // customer ID
                    TCPMessage request = new TCPMessage("queryCustomer", params);
                    synchronized (out) {
                        out.writeObject(request);
                        out.flush();
                    }
                    TCPMessage response = (TCPMessage) in.readObject();
                    System.out.println(response.getStringResponse());
                    break;
                }

                case QueryFlightPrice: {
                    checkArgumentsCount(2, arguments.size());
                    System.out.println("Querying a flight price");
                    System.out.println("-Flight Number: " + arguments.elementAt(1));

                    Vector<String> params = new Vector<>();
                    params.add(arguments.elementAt(1)); // flight number
                    TCPMessage request = new TCPMessage("queryFlightPrice", params);
                    synchronized (out) {
                        out.writeObject(request);
                        out.flush();
                    }
                    TCPMessage response = (TCPMessage) in.readObject();
                    System.out.println("Price of a seat: " + response.getIntResponse());
                    break;
                }

                case QueryCarsPrice: {
                    checkArgumentsCount(2, arguments.size());
                    System.out.println("Querying cars price");
                    System.out.println("-Car Location: " + arguments.elementAt(1));

                    Vector<String> params = new Vector<>();
                    params.add(arguments.elementAt(1)); // location
                    TCPMessage request = new TCPMessage("queryCarsPrice", params);
                    synchronized (out) {
                        out.writeObject(request);
                        out.flush();
                    }
                    TCPMessage response = (TCPMessage) in.readObject();
                    System.out.println("Price of cars at this location: " + response.getIntResponse());
                    break;
                }

                case QueryRoomsPrice: {
                    checkArgumentsCount(2, arguments.size());
                    System.out.println("Querying rooms price");
                    System.out.println("-Room Location: " + arguments.elementAt(1));

                    Vector<String> params = new Vector<>();
                    params.add(arguments.elementAt(1)); // location
                    TCPMessage request = new TCPMessage("queryRoomsPrice", params);
                    synchronized (out) {
                        out.writeObject(request);
                        out.flush();
                    }
                    TCPMessage response = (TCPMessage) in.readObject();
                    System.out.println("Price of rooms at this location: " + response.getIntResponse());
                    break;
                }

                case ReserveFlight: {
                    checkArgumentsCount(3, arguments.size());
                    System.out.println("Reserving seat in a flight");
                    System.out.println("-Customer ID: " + arguments.elementAt(1));
                    System.out.println("-Flight Number: " + arguments.elementAt(2));

                    Vector<String> params = new Vector<>();
                    params.add(arguments.elementAt(1)); // customer ID
                    params.add(arguments.elementAt(2)); // flight number
                    TCPMessage request = new TCPMessage("reserveFlight", params);
                    synchronized (out) {
                        out.writeObject(request);
                        out.flush();
                    }
                    TCPMessage response = (TCPMessage) in.readObject();
                    System.out.println(response.getBoolResponse() ? "Flight Reserved" : "Flight could not be reserved");
                    break;
                }

                case ReserveCar: {
                    checkArgumentsCount(3, arguments.size());
                    System.out.println("Reserving a car at a location");
                    System.out.println("-Customer ID: " + arguments.elementAt(1));
                    System.out.println("-Car Location: " + arguments.elementAt(2));

                    Vector<String> params = new Vector<>();
                    params.add(arguments.elementAt(1)); // customer ID
                    params.add(arguments.elementAt(2)); // location
                    TCPMessage request = new TCPMessage("reserveCar", params);
                    synchronized (out) {
                        out.writeObject(request);
                        out.flush();
                    }
                    TCPMessage response = (TCPMessage) in.readObject();
                    System.out.println(response.getBoolResponse() ? "Car Reserved" : "Car could not be reserved");
                    break;
                }

                case ReserveRoom: {
                    checkArgumentsCount(3, arguments.size());
                    System.out.println("Reserving a room at a location");
                    System.out.println("-Customer ID: " + arguments.elementAt(1));
                    System.out.println("-Room Location: " + arguments.elementAt(2));

                    Vector<String> params = new Vector<>();
                    params.add(arguments.elementAt(1)); // customer ID
                    params.add(arguments.elementAt(2)); // location
                    TCPMessage request = new TCPMessage("reserveRoom", params);
                    synchronized (out) {
                        out.writeObject(request);
                        out.flush();
                    }
                    TCPMessage response = (TCPMessage) in.readObject();
                    System.out.println(response.getBoolResponse() ? "Room Reserved" : "Room could not be reserved");
                    break;
                }

                case Bundle: {
                    if (arguments.size() < 5) {
                        System.err.println((char) 27 + "[31m" + "Missing arguments" + (char) 27 + "[0m");
                        break;
                    }

                    System.out.println("Reserving an atomic bundle");
                    System.out.println("-Customer ID: " + arguments.elementAt(1));
                    
                    // Display flight numbers
                    System.out.print("-Flight Numbers: ");
                    for (int i = 2; i < arguments.size() - 3; i++) {
                        System.out.print(arguments.elementAt(i));
                        if (i < arguments.size() - 4) System.out.print(", ");
                    }
                    System.out.println();
                    
                    System.out.println("-Location: " + arguments.elementAt(arguments.size() - 3));
                    System.out.println("-Book Car: " + (arguments.elementAt(arguments.size() - 2).equals("1") ? "Yes" : "No"));
                    System.out.println("-Book Room: " + (arguments.elementAt(arguments.size() - 1).equals("1") ? "Yes" : "No"));

                    Vector<String> params = new Vector<>();
                    for (int i = 1; i < arguments.size(); i++) {
                        params.add(arguments.elementAt(i));
                    }
                    TCPMessage request = new TCPMessage("bundle", params);
                    synchronized (out) {
                        out.writeObject(request);
                        out.flush();
                    }
                    TCPMessage response = (TCPMessage) in.readObject();
                    System.out.println(response.getBoolResponse() ? "Bundle reserved successfully" : "Bundle could not be reserved");
                    break;
                }

                case Quit: {
                    checkArgumentsCount(1, arguments.size());
                    System.out.println("Quitting client");
                    System.exit(0);
                    break;
                }

                case Help: {
                    if (arguments.size() == 1) {
                        System.out.println(Command.description());
                    } else if (arguments.size() == 2) {
                        Command l_cmd = Command.fromString((String)arguments.elementAt(1));
                        System.out.println(l_cmd.toString());
                    } else {
                        System.err.println((char)27 + "[31;1mCommand exception: " + (char)27 + "[0mImproper use of help command. Location \"help\" or \"help,<CommandName>\"");
                    }
                    break;
			    }

                default:
                    System.out.println("Command not implemented in TCP client");
        }
    } catch (Exception e) {
        System.err.println("Error in execute: " + e.getMessage());
        e.printStackTrace();
    }
}



    public static void main(String[] args) {
        if(args.length > 0) {
            middlewareHost = args[0];
        }
        if(args.length > 1) {
            middlewarePort = Integer.parseInt(args[1]);
        }
        TCP_Client client = new TCP_Client(middlewareHost, middlewarePort);
        client.connectServer(); // Implement this
        client.start(); // Override start() from Client.java ?
        
    }
    
}