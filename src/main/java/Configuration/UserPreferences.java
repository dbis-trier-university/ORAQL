package Configuration;

public class UserPreferences {
    private double minReliability;
    private int maxExecutionTime;

    /*******************************************************************************************************************
     * Constructor
     ******************************************************************************************************************/

    public UserPreferences(double minReliability, int maxExecutionTime) {
        this.minReliability = minReliability;
        this.maxExecutionTime = maxExecutionTime;
    }

    /*******************************************************************************************************************
     * Public Methods
     ******************************************************************************************************************/

    public double getMinReliability() {
        return minReliability;
    }

    public int getMaxExecutionTime() {
        return maxExecutionTime;
    }
}
