package Server.RMI;

import Server.Common.ResourceManager;
import Server.Interface.*;
import java.rmi.RemoteException;
import java.util.Vector;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.ThreadPoolExecutor;
import javax.swing.tree.RowMapper;

public class RMIMiddleware implements IResourceManager {

    private static String s_serverName = "Middleware";
	private static String s_rmiPrefix = "group_38_";
    private IResourceManager flightsRM;
    private IResourceManager carsRM;
    private IResourceManager roomsRM;

    private static int customerID = 0;
    //private IResourceManager customersRM = new RMIResourceManager(s_rmiPrefix + "Customers");


    public RMIMiddleware(
        IResourceManager flightsRM,
        IResourceManager carsRM,
        IResourceManager roomsRM
    ) {
        this.flightsRM = flightsRM;
        this.carsRM = carsRM;
        this.roomsRM = roomsRM;
    }

    @Override
    public boolean addFlight(int flightNum, int flightSeats, int flightPrice) throws RemoteException {
        return this.flightsRM.addFlight(flightNum, flightSeats, flightPrice);
    }

    @Override
    public boolean addCars(String location, int numCars, int price) throws RemoteException {
        return this.carsRM.addCars(location, numCars, price);
    }

    @Override
    public boolean addRooms(String location, int numRooms, int price) throws RemoteException {
        return this.roomsRM.addRooms(location, numRooms, price);
    }

    @Override
    public int newCustomer() throws RemoteException {
        int newCustomerID = RMIMiddleware.customerID; // create unique userID
        this.flightsRM.newCustomer(newCustomerID);
        this.carsRM.newCustomer(newCustomerID);
        this.roomsRM.newCustomer(newCustomerID);
        RMIMiddleware.customerID += 1;
        return newCustomerID;
    }

    @Override
    public boolean newCustomer(int cid) throws RemoteException {
        return this.flightsRM.newCustomer(cid)
        && this.carsRM.newCustomer(cid) && this.roomsRM.newCustomer(cid);
    }

    @Override
    public boolean deleteFlight(int flightNum) throws RemoteException {
        return this.flightsRM.deleteFlight(flightNum);
    }

    @Override
    public boolean deleteCars(String location) throws RemoteException {
        return this.carsRM.deleteCars(location);
    }

    @Override
    public boolean deleteRooms(String location) throws RemoteException {
        return this.roomsRM.deleteRooms(location);
    }

    @Override
    public boolean deleteCustomer(int customerID) throws RemoteException {
        return this.flightsRM.deleteCustomer(customerID)
        && this.carsRM.deleteCustomer(customerID) && this.roomsRM.deleteCustomer(customerID);
    }

    @Override
    public int queryFlight(int flightNumber) throws RemoteException {
        return this.flightsRM.queryFlight(flightNumber);
    }

    @Override
    public int queryCars(String location) throws RemoteException {
        return this.carsRM.queryCars(location);
    }

    @Override
    public int queryRooms(String location) throws RemoteException {
        return this.roomsRM.queryRooms(location);
    }

    @Override
    public String queryCustomerInfo(int customerID) throws RemoteException {
        // TODO: make it look like one bill
        return "Bill for customer " + customerID + "\n" +
        this.flightsRM.queryCustomerInfo(customerID) +
        this.carsRM.queryCustomerInfo(customerID) +
        this.roomsRM.queryCustomerInfo(customerID);
    }

    @Override
    public int queryFlightPrice(int flightNumber) throws RemoteException {
        return this.flightsRM.queryFlightPrice(flightNumber);
    }

    @Override
    public int queryCarsPrice(String location) throws RemoteException {
        return this.carsRM.queryCarsPrice(location);
    }

    @Override
    public int queryRoomsPrice(String location) throws RemoteException {
        return this.roomsRM.queryRoomsPrice(location);
    }

    @Override
    public boolean reserveFlight(int customerID, int flightNumber) throws RemoteException {
        return this.flightsRM.reserveFlight(customerID, flightNumber);
    }

    @Override
    public boolean reserveCar(int customerID, String location) throws RemoteException {
        return this.carsRM.reserveCar(customerID, location);
    }

    @Override
    public boolean reserveRoom(int customerID, String location) throws RemoteException {
        return this.roomsRM.reserveRoom(customerID, location);
    }

    @Override
    public synchronized boolean bundle(int customerID, Vector<String> flightNumbers, String location, boolean car, boolean room) throws RemoteException {
        // Attempt to book room and car first if indicated
        if ((room && this.roomsRM.queryRooms(location) == 0) || (car && this.carsRM.queryCars(location) == 0)) {
            return false;
        }
        this.roomsRM.reserveRoom(customerID, location);
        this.carsRM.reserveCar(customerID, location);

        // First ensure that ALL flightNumbers have seats available
        for (String flightNumStr : flightNumbers) {
            int flightNum = Integer.parseInt(flightNumStr);
            // If a single flight has no seats, do not book any flights and return false
            if(this.flightsRM.queryFlight(flightNum) == 0) {
                return false;
            }
        }

        // Otherwise book all flights
        for (String flightNumStr : flightNumbers) {
            int flightNum = Integer.parseInt(flightNumStr);
            this.flightsRM.reserveFlight(customerID, flightNum);
        }
        return true;
    }

    @Override
    public String getName() throws RemoteException {
        return s_serverName;
    }

    public static void main(String[] args) {
        try {
            // Get references to the registry for ResourceManagers
            // All ResourceManagers use the same RMI registry

            // Flights RMI registry
            Registry flightsRMRegistry = LocateRegistry.getRegistry(args[0], 3038);
            Registry carsRMRegistry = LocateRegistry.getRegistry(args[1], 3038);
            Registry roomsRMRegistry = LocateRegistry.getRegistry(args[2], 3038);

            // Using above registries, get remote objects associated with each ResourceManager
            IResourceManager flightRM = (IResourceManager) flightsRMRegistry.lookup(s_rmiPrefix + "Flights");
            IResourceManager carRM = (IResourceManager) carsRMRegistry.lookup(s_rmiPrefix + "Cars"); 
            IResourceManager roomRM = (IResourceManager) roomsRMRegistry.lookup(s_rmiPrefix + "Rooms");  
            
			RMIMiddleware middleware_server = new RMIMiddleware(
                flightRM, carRM, roomRM
            );

			// Dynamically generate the RMIMiddleware proxy for the actual clients
			IResourceManager middlewareserver_stub = (IResourceManager)UnicastRemoteObject.exportObject(middleware_server, 0);

			// Bind the remote object's stub in the registry at port 3038
            // Provides proxy for the middleware stub to clients who are
            // connecting to server via port 3038
			Registry l_registry;
			try {
				l_registry = LocateRegistry.createRegistry(3038);
			} catch (RemoteException e) {
				l_registry = LocateRegistry.getRegistry(3038);
			}
			final Registry registry = l_registry;
            // "group_38_Middleware" binds to middleware stub obj
			registry.bind(s_rmiPrefix + s_serverName, middlewareserver_stub);

            // For shutdown
            Runtime.getRuntime().addShutdownHook(new Thread() {
				public void run() {
					try {
						registry.unbind(s_rmiPrefix + s_serverName);
						System.out.println("'" + s_serverName + "' resource manager unbound");
					}
					catch(Exception e) {
						System.err.println((char)27 + "[31;1mServer exception: " + (char)27 + "[0mUncaught exception");
						e.printStackTrace();
					}
				}
			});                                       
			System.out.println("'" + s_serverName + "' resource manager server ready and bound to '" + s_rmiPrefix + s_serverName + "'");

        } catch (Exception e) {
            System.err.print("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }

}