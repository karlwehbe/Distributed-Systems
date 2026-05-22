package Server.Common;

import java.util.Vector;
import java.io.Serializable;

public class TCPMessage implements Serializable {
    private String methodName;
    private Vector<String> parameters;
    private boolean isRequest = false;
    private int responseIntVal;
    private boolean responseBoolVal;
    private String responseStringVal;

    // For TCP requests
    public TCPMessage(String methodName, Vector parameters) {
        this.methodName = methodName;
        this.parameters = parameters;
        this.isRequest = true;
    }

    // For TCP responses with integer response values
    public TCPMessage(int intResponse) {
        this.responseIntVal = intResponse;
    }

    // For TCP responses with boolean response values
    public TCPMessage(boolean boolResponse) {
        this.responseBoolVal = boolResponse;
    }

    // For TCP responses with String response values
    public TCPMessage(String stringResponse) {
        this.responseStringVal = stringResponse;
    }

    public String getMethodName() {
        return this.methodName;
    }

    public Vector<String> getParameters() {
        return this.parameters;
    }

    public boolean getBoolResponse() {
        return this.responseBoolVal;
    }

    public int getIntResponse() {
        return this.responseIntVal;
    }

    public String getStringResponse() {
        return this.responseStringVal;
    }
    
}