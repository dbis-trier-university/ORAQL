package QueryManagement.Utils;

public class Endpoint {
    private String label, name, url;
    private double reliability;
    private int time;

    /*******************************************************************************************************************
     * Constructor
     ******************************************************************************************************************/

    public Endpoint(String label, String name, String url) {
        // Call constructor with default values
        this(label,name,url,0.5,0.5,1000);
    }

    public Endpoint(String label, String name, String url, double reliability, double coverage, int time) {
        this.label = label;
        this.name = name;
        this.url = url;

        this.reliability = reliability;
        this.time = time;
    }

    public Endpoint(String label, String name, String url, double reliability, int time) {
        this(label,name,url,reliability,0,time);
    }

    /*******************************************************************************************************************
     * Public Methods
     ******************************************************************************************************************/

    public String getLabel() {
        return label;
    }

    public String getName() {
        return name;
    }

    public String getUrl() {
        return url;
    }

    public double getReliability() {
        return reliability;
    }



    public int getTime() {
        return time;
    }



    public String toString(){
        return "{" + getLabel() + ", " + getReliability() + ", " + getTime() + "}";
    }
}
