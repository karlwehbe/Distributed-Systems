package Server.TCP;

import Server.Common.*;
import Server.Interface.*;
import java.util.*;
import java.io.*;
import java.net.*;

public class TCP_Middleware {
    private static String s_serverName = "TCP_Middleware";
	private static String s_rmiPrefix = "group_38_";
    private static int s_serverPort = 3038;
    
    private ServerSocket serverSocket; // Listens for incoming TCPClient

    protected TCP_Proxy flights_TCP_proxy = null;
    protected TCP_Proxy cars_TCP_proxy = null;
    protected TCP_Proxy rooms_TCP_proxy = null;

    private static int customerID = 0;
    private static HashMap<Integer, Customer> customers = new HashMap<>();

    public TCP_Middleware(String flightHostname, int flightPort, String carHostname, int carPort, String roomHostname, int roomPort) {
        flights_TCP_proxy = new TCP_Proxy(flightHostname, flightPort);
        System.out.println("Connection established with FlightsRM at " + flightHostname + ":" + flightPort);
        cars_TCP_proxy = new TCP_Proxy(carHostname, carPort);
        System.out.println("Connection established with CarsRM at " + carHostname + ":" + carPort);
        rooms_TCP_proxy = new TCP_Proxy(roomHostname, roomPort);
        System.out.println("Connection established with RoomsRM at " + roomHostname + ":" + roomPort);

    }

    // Customer management methods
    public synchronized TCPMessage handleAddCustomer(TCPMessage request) {
        try {
            customerID++;
            Customer customer = new Customer(customerID);
            customers.put(customerID, customer);
            
            // Create customer in all resource managers
            createCustomerInAllRMs(customerID);
            
            System.out.println("Added customer with ID: " + customerID);
            return new TCPMessage(customerID);
        } catch (Exception e) {
            System.err.println("Error adding customer: " + e.getMessage());
            return new TCPMessage(0);
        }
    }

    public synchronized TCPMessage handleAddCustomerID(TCPMessage request) {
        try {
            Vector<String> params = request.getParameters();
            int id = Integer.parseInt(params.elementAt(0));
            
            if (customers.containsKey(id)) {
                System.err.println("Customer ID " + id + " already exists");
                return new TCPMessage(false);
            }
            
            Customer customer = new Customer(id);
            customers.put(id, customer);
            
            // Create customer in all resource managers
            createCustomerInAllRMs(id);
            
            System.out.println("Added customer with ID: " + id);
            return new TCPMessage(true);
        } catch (Exception e) {
            System.err.println("Error adding customer with ID: " + e.getMessage());
            return new TCPMessage(false);
        }
    }

    public synchronized TCPMessage handleDeleteCustomer(TCPMessage request) {
        try {
            Vector<String> params = request.getParameters();
            int id = Integer.parseInt(params.elementAt(0));
            
            if (customers.remove(id) != null) {
                // Delete customer from all resource managers
                deleteCustomerFromAllRMs(id);
                
                System.out.println("Deleted customer with ID: " + id);
                return new TCPMessage(true);
            } else {
                System.err.println("Customer ID " + id + " not found");
                return new TCPMessage(false);
            }
        } catch (Exception e) {
            System.err.println("Error deleting customer: " + e.getMessage());
            return new TCPMessage(false);
        }
    }

    public synchronized TCPMessage handleQueryCustomer(TCPMessage request) {
        try {
            Vector<String> params = request.getParameters();
            int id = Integer.parseInt(params.elementAt(0));
            
            Customer customer = customers.get(id);
            if (customer != null) {
                String bill = customer.getBill();
                System.out.println("Customer " + id + " bill: " + bill);
                return new TCPMessage(bill);
            } else {
                System.err.println("Customer ID " + id + " not found");
                return new TCPMessage("Customer not found");
            }
        } catch (Exception e) {
            System.err.println("Error querying customer: " + e.getMessage());
            return new TCPMessage("Error querying customer");
        }
    }

    // Helper method to create customer in all resource managers
    private void createCustomerInAllRMs(int customerID) {
        try {
            // Create customer in Flight RM
            Vector<String> flightParams = new Vector<>();
            flightParams.add(String.valueOf(customerID));
            TCPMessage flightRequest = new TCPMessage("addCustomerID", flightParams);
            flights_TCP_proxy.send_TCP_request(flightRequest);
            
            // Create customer in Car RM
            Vector<String> carParams = new Vector<>();
            carParams.add(String.valueOf(customerID));
            TCPMessage carRequest = new TCPMessage("addCustomerID", carParams);
            cars_TCP_proxy.send_TCP_request(carRequest);
            
            // Create customer in Room RM
            Vector<String> roomParams = new Vector<>();
            roomParams.add(String.valueOf(customerID));
            TCPMessage roomRequest = new TCPMessage("addCustomerID", roomParams);
            rooms_TCP_proxy.send_TCP_request(roomRequest);
            
            System.out.println("Created customer " + customerID + " in all resource managers");
        } catch (Exception e) {
            System.err.println("Error creating customer in resource managers: " + e.getMessage());
        }
    }

    // Helper method to delete customer from all resource managers
    private void deleteCustomerFromAllRMs(int customerID) {
        try {
            // Delete customer from Flight RM
            Vector<String> flightParams = new Vector<>();
            flightParams.add(String.valueOf(customerID));
            TCPMessage flightRequest = new TCPMessage("deleteCustomer", flightParams);
            flights_TCP_proxy.send_TCP_request(flightRequest);
            
            // Delete customer from Car RM
            Vector<String> carParams = new Vector<>();
            carParams.add(String.valueOf(customerID));
            TCPMessage carRequest = new TCPMessage("deleteCustomer", carParams);
            cars_TCP_proxy.send_TCP_request(carRequest);
            
            // Delete customer from Room RM
            Vector<String> roomParams = new Vector<>();
            roomParams.add(String.valueOf(customerID));
            TCPMessage roomRequest = new TCPMessage("deleteCustomer", roomParams);
            rooms_TCP_proxy.send_TCP_request(roomRequest);
            
            System.out.println("Deleted customer " + customerID + " from all resource managers");
        } catch (Exception e) {
            System.err.println("Error deleting customer from resource managers: " + e.getMessage());
        }
    }

    // Reservation handling methods
    public synchronized TCPMessage handleReserveFlight(TCPMessage request) {
        try {
            Vector<String> params = request.getParameters();
            int customerID = Integer.parseInt(params.elementAt(0));
            int flightNum = Integer.parseInt(params.elementAt(1));
            
            if (!customers.containsKey(customerID)) {
                System.err.println("Customer ID " + customerID + " not found");
                return new TCPMessage(false);
            }
            
            // Get flight price first
            Vector<String> priceParams = new Vector<>();
            priceParams.add(String.valueOf(flightNum));
            TCPMessage priceRequest = new TCPMessage("queryFlightPrice", priceParams);
            TCPMessage priceResponse = flights_TCP_proxy.send_TCP_request(priceRequest);
            int flightPrice = (priceResponse != null) ? priceResponse.getIntResponse() : 0;
            
            TCPMessage response = flights_TCP_proxy.send_TCP_request(request);
            
            if (response != null && response.getBoolResponse()) {
                Customer customer = customers.get(customerID);
                customer.reserve("flight-" + flightNum, "flight", flightPrice);
                System.out.println("Flight " + flightNum + " reserved for customer " + customerID + " at price $" + flightPrice);
                return new TCPMessage(true);
            } else {
                System.err.println("Failed to reserve flight " + flightNum + " for customer " + customerID);
                return new TCPMessage(false);
            }
        } catch (Exception e) {
            System.err.println("Error reserving flight: " + e.getMessage());
            return new TCPMessage(false);
        }
    }

    public synchronized TCPMessage handleReserveCar(TCPMessage request) {
        try {
            Vector<String> params = request.getParameters();
            int customerID = Integer.parseInt(params.elementAt(0));
            String location = params.elementAt(1);
            
            // Check if customer exists
            if (!customers.containsKey(customerID)) {
                System.err.println("Customer ID " + customerID + " not found");
                return new TCPMessage(false);
            }
            
            // Get car price first
            Vector<String> priceParams = new Vector<>();
            priceParams.add(location);
            TCPMessage priceRequest = new TCPMessage("queryCarsPrice", priceParams);
            TCPMessage priceResponse = cars_TCP_proxy.send_TCP_request(priceRequest);
            int carPrice = (priceResponse != null) ? priceResponse.getIntResponse() : 0;
            
            TCPMessage response = cars_TCP_proxy.send_TCP_request(request);
            
            if (response != null && response.getBoolResponse()) {
                Customer customer = customers.get(customerID);
                customer.reserve("car-" + location, location, carPrice);
                System.out.println("Car at " + location + " reserved for customer " + customerID + " at price $" + carPrice);
                return new TCPMessage(true);
            } else {
                System.err.println("Failed to reserve car at " + location + " for customer " + customerID);
                return new TCPMessage(false);
            }
        } catch (Exception e) {
            System.err.println("Error reserving car: " + e.getMessage());
            return new TCPMessage(false);
        }
    }

    public synchronized TCPMessage handleReserveRoom(TCPMessage request) {
        try {
            Vector<String> params = request.getParameters();
            int customerID = Integer.parseInt(params.elementAt(0));
            String location = params.elementAt(1);
            
            if (!customers.containsKey(customerID)) {
                System.err.println("Customer ID " + customerID + " not found");
                return new TCPMessage(false);
            }
            
            // Get room price first
            Vector<String> priceParams = new Vector<>();
            priceParams.add(location);
            TCPMessage priceRequest = new TCPMessage("queryRoomsPrice", priceParams);
            TCPMessage priceResponse = rooms_TCP_proxy.send_TCP_request(priceRequest);
            int roomPrice = (priceResponse != null) ? priceResponse.getIntResponse() : 0;
            
            TCPMessage response = rooms_TCP_proxy.send_TCP_request(request);
            
            if (response != null && response.getBoolResponse()) {
                Customer customer = customers.get(customerID);
                customer.reserve("room-" + location, location, roomPrice);
                System.out.println("Room at " + location + " reserved for customer " + customerID + " at price $" + roomPrice);
                return new TCPMessage(true);
            } else {
                System.err.println("Failed to reserve room at " + location + " for customer " + customerID);
                return new TCPMessage(false);
            }
        } catch (Exception e) {
            System.err.println("Error reserving room: " + e.getMessage());
            return new TCPMessage(false);
        }
    }

    // Bundle reservation handling method
    public synchronized TCPMessage handleBundle(TCPMessage request) {
        try {
            Vector<String> params = request.getParameters();
            
            if (params.size() < 5) {
                System.err.println("Bundle command requires at least 5 parameters: customerID, flightNumbers, location, bookCar, bookRoom");
                return new TCPMessage(false);
            }
            
            int customerID = Integer.parseInt(params.elementAt(0));
            
            if (!customers.containsKey(customerID)) {
                System.err.println("Customer ID " + customerID + " not found");
                return new TCPMessage(false);
            }
            
            // Parse flight numbers (all parameters except last 3: location, bookCar, bookRoom)
            Vector<Integer> flightNumbers = new Vector<>();
            for (int i = 1; i < params.size() - 3; i++) {
                flightNumbers.add(Integer.parseInt(params.elementAt(i)));
            }
            
            String location = params.elementAt(params.size() - 3);
            boolean bookCar = Integer.parseInt(params.elementAt(params.size() - 2)) == 1;
            boolean bookRoom = Integer.parseInt(params.elementAt(params.size() - 1)) == 1;
            
            System.out.println("Processing bundle for customer " + customerID + 
                             ", flights: " + flightNumbers + 
                             ", location: " + location + 
                             ", bookCar: " + bookCar + 
                             ", bookRoom: " + bookRoom);
            
            Vector<Integer> flightPrices = new Vector<>();
            int carPrice = 0;
            int roomPrice = 0;
            
            // Reserve all flights first
            for (int flightNum : flightNumbers) {
                Vector<String> priceParams = new Vector<>();
                priceParams.add(String.valueOf(flightNum));
                TCPMessage priceRequest = new TCPMessage("queryFlightPrice", priceParams);
                TCPMessage priceResponse = flights_TCP_proxy.send_TCP_request(priceRequest);
                int flightPrice = (priceResponse != null) ? priceResponse.getIntResponse() : 0;
                flightPrices.add(flightPrice);
                
                Vector<String> flightParams = new Vector<>();
                flightParams.add(String.valueOf(customerID));
                flightParams.add(String.valueOf(flightNum));
                TCPMessage flightRequest = new TCPMessage("reserveFlight", flightParams);
                TCPMessage flightResponse = flights_TCP_proxy.send_TCP_request(flightRequest);
                
                if (flightResponse == null || !flightResponse.getBoolResponse()) {
                    System.err.println("Failed to reserve flight " + flightNum + " for customer " + customerID);
                    return new TCPMessage(false);
                }
            }
            
            // Reserve car if true
            if (bookCar) {
                Vector<String> priceParams = new Vector<>();
                priceParams.add(location);
                TCPMessage priceRequest = new TCPMessage("queryCarsPrice", priceParams);
                TCPMessage priceResponse = cars_TCP_proxy.send_TCP_request(priceRequest);
                carPrice = (priceResponse != null) ? priceResponse.getIntResponse() : 0;
                
                Vector<String> carParams = new Vector<>();
                carParams.add(String.valueOf(customerID));
                carParams.add(location);
                TCPMessage carRequest = new TCPMessage("reserveCar", carParams);
                TCPMessage carResponse = cars_TCP_proxy.send_TCP_request(carRequest);
                
                if (carResponse == null || !carResponse.getBoolResponse()) {
                    System.err.println("Failed to reserve car at " + location + " for customer " + customerID);
                    return new TCPMessage(false);
                }
            }
            
            // Reserve room if true
            if (bookRoom) {
                Vector<String> priceParams = new Vector<>();
                priceParams.add(location);
                TCPMessage priceRequest = new TCPMessage("queryRoomsPrice", priceParams);
                TCPMessage priceResponse = rooms_TCP_proxy.send_TCP_request(priceRequest);
                roomPrice = (priceResponse != null) ? priceResponse.getIntResponse() : 0;
                
                Vector<String> roomParams = new Vector<>();
                roomParams.add(String.valueOf(customerID));
                roomParams.add(location);
                TCPMessage roomRequest = new TCPMessage("reserveRoom", roomParams);
                TCPMessage roomResponse = rooms_TCP_proxy.send_TCP_request(roomRequest);
                
                if (roomResponse == null || !roomResponse.getBoolResponse()) {
                    System.err.println("Failed to reserve room at " + location + " for customer " + customerID);
                    return new TCPMessage(false);
                }
            }
            
            // Update customer's reservations in middleware with actual prices
            Customer customer = customers.get(customerID);
            for (int i = 0; i < flightNumbers.size(); i++) {
                int flightNum = flightNumbers.elementAt(i);
                int price = flightPrices.elementAt(i);
                customer.reserve("flight-" + flightNum, "flight", price);
            }
            if (bookCar) {
                customer.reserve("car-" + location, location, carPrice);
            }
            if (bookRoom) {
                customer.reserve("room-" + location, location, roomPrice);
            }
            
            System.out.println("Bundle reservation successful for customer " + customerID);
            return new TCPMessage(true);
            
        } catch (Exception e) {
            System.err.println("Error processing bundle reservation: " + e.getMessage());
            e.printStackTrace();
            return new TCPMessage(false);
        }
    }

    public static void main(String[] args) {
        try {
            // Get resource manager hostnames and ports from command line arguments
            if (args.length != 6) {
                System.err.println("Usage: java TCP_Middleware <flights_host> <flights_port> <cars_host> <cars_port> <rooms_host> <rooms_port>");
                System.exit(1);
            }
            
            String flightsHost = args[0];
            int flightsPort = Integer.parseInt(args[1]);
            String carsHost = args[2];
            int carsPort = Integer.parseInt(args[3]);
            String roomsHost = args[4];
            int roomsPort = Integer.parseInt(args[5]);
            
            TCP_Middleware middleware = new TCP_Middleware(flightsHost, flightsPort, carsHost, carsPort, roomsHost, roomsPort);
            
            ServerSocket serverSocket = new ServerSocket(s_serverPort);
            System.out.println(s_rmiPrefix + s_serverName + " listening on port: " + s_serverPort);
            
            Runtime.getRuntime().addShutdownHook(new Thread() {
				public void run() {
                    try {
                        serverSocket.close();
                        System.out.println("TCP Middleware server closed");
                    } catch(IOException e) {
                        System.err.println("Error closing server: " + e.getMessage());
                    }
                }
            });
            
            while (true) {
                new ClientHandler(serverSocket.accept(), middleware).start();
            }

        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
        }

        
    // ClientHandler class to handle client connections
        private static class ClientHandler extends Thread {
        private Socket clientSocket;
        private TCP_Middleware middleware;
        private ObjectInputStream inputBuffer;
        private ObjectOutputStream outputBuffer;

        public ClientHandler(Socket socket, TCP_Middleware middleware) {
            this.clientSocket = socket;
            this.middleware = middleware;
        }

        @Override
        public void run() {
            try {
                System.out.println("Received a message from client");
                this.outputBuffer = new ObjectOutputStream(clientSocket.getOutputStream());
                this.inputBuffer = new ObjectInputStream(clientSocket.getInputStream());
                
                while (true) {
                    TCPMessage request = (TCPMessage) inputBuffer.readObject();
                    TCPMessage response = route(request);
                    try {
                        outputBuffer.writeObject(response);
                        outputBuffer.flush();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }           
            } catch(ClassNotFoundException e) {
                e.printStackTrace();
            } catch(IOException ex) {
                System.out.println("A client has disconnected");
            } finally {
                // Gracefully close TCPClient connection
                try {
                    if (clientSocket != null) {
                    clientSocket.close();
                } 
                } catch (EOFException e) {
                    System.out.println("Client disconnected: " + clientSocket.getRemoteSocketAddress());
                } catch (IOException e) {
                    e.printStackTrace();
                }
                
            }
        }

        private TCPMessage route(TCPMessage request) {
            CommandEnum command;

            try {
                command = CommandEnum.valueOf(request.getMethodName());
            } catch (IllegalArgumentException e) {
                return new TCPMessage(false); // invalid command
            }

            try {
                switch (command) {
                    case addFlight:
                    case deleteFlight:
                    case queryFlight:
                    case queryFlightPrice:
                        return middleware.flights_TCP_proxy.send_TCP_request(request);

                    case reserveFlight:
                        return middleware.handleReserveFlight(request);
                    
                    case addCars:
                    case deleteCars:
                    case queryCars:
                    case queryCarsPrice:
                        return middleware.cars_TCP_proxy.send_TCP_request(request);

                    case reserveCar:
                        return middleware.handleReserveCar(request);

                    case addRooms:
                    case deleteRooms:
                    case queryRooms:
                    case queryRoomsPrice:
                        return middleware.rooms_TCP_proxy.send_TCP_request(request);

                    case reserveRoom:
                        return middleware.handleReserveRoom(request);

                    case addCustomer:
                        return middleware.handleAddCustomer(request);
                    
                    case addCustomerID:
                        return middleware.handleAddCustomerID(request);
                    
                    case deleteCustomer:
                        return middleware.handleDeleteCustomer(request);
                    
                    case queryCustomer:
                        return middleware.handleQueryCustomer(request);

                    case bundle:
                        return middleware.handleBundle(request);

                    default:
                        return new TCPMessage(false);
                }
            } catch (Exception e) {
                System.err.println("Error handling method " + request.getMethodName() + ": " + e.getMessage());
                return new TCPMessage(false);
            }
        }
    }

}