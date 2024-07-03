package QueryManagement.Indexer.Index.Characteristics;

import java.util.HashMap;
import java.util.Map;

public class Info {
    private long counter;
    private double coverage;
    private Map<String,Double> multiplicityMap;

    /*******************************************************************************************************************
     * Constructor
     ******************************************************************************************************************/

    public Info() {
        this.counter = 1;
        this.multiplicityMap = new HashMap<>();
    }

    /*******************************************************************************************************************
     * Public Methods
     ******************************************************************************************************************/

    public long getCounter() {
        return counter;
    }

    public double getCoverage() {
        return coverage;
    }

    public Map<String,Double> getMultiplicityMap() {
        return this.multiplicityMap;
    }

    public void increaseCounter(){
        this.counter++;
    }

    public void setMultiplicityMap(Map<String,Double> multiplicityMap) {
        this.multiplicityMap = multiplicityMap;
    }

    public void setCoverage(double coverage) {
        this.coverage = coverage;
    }
}
