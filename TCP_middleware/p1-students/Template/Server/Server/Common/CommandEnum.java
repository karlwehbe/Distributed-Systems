package Server.Common;

public enum CommandEnum {
    addFlight("addFlight"),
    addCars("addCars"),
    addRooms("addRooms"),
    addCustomer("addCustomer"),
    addCustomerID("addCustomerID"),

    deleteFlight("deleteFlight"),
    deleteCars("deleteCars"),
    deleteRooms("deleteRooms"),
    deleteCustomer("deleteCustomer"),

    queryFlight("queryFlight"),
    queryCars("queryCars"),
    queryRooms("queryRooms"),
    queryCustomer("queryCustomer"),

    queryFlightPrice("queryFlightPrice"),
    queryCarsPrice("queryCarsPrice"),
    queryRoomsPrice("queryRoomsPrice"),

    reserveFlight("reserveFlight"),
    reserveCar("reserveCar"),
    reserveRoom("reserveRoom"),

    cancelFlight("cancelFlight"),
    cancelCar("cancelCar"),
    cancelRoom("cancelRoom"),

    bundle("bundle"),
    quit("quit");

    private final String value;

    CommandEnum(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}