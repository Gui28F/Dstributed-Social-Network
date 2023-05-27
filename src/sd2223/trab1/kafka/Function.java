package sd2223.trab1.kafka;

import java.io.Serializable;

public class Function implements Serializable {
    private String functionName;
    private Object[] parameters;

    public Function(String functionName, Object[] parameters) {
        this.functionName = functionName;
        this.parameters = parameters;
    }

    public Function() {
        // Default constructor
    }

    public String getFunctionName() {
        return functionName;
    }

    public void setFunctionName(String functionName) {
        this.functionName = functionName;
    }

    public Object[] getParameters() {
        return parameters;
    }

    public void setParameters(Object[] parameters) {
        this.parameters = parameters;
    }
}
