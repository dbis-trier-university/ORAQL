package QueryManagement.Indexer.Overlap;

public class OverlapInfo {
    private String ep1, ep2;
    private double overlap;
    private int requests;

    /*******************************************************************************************************************
     * Constructor
     ******************************************************************************************************************/

    public OverlapInfo(String ep1, String ep2) {
        this.ep1 = ep1;
        this.ep2 = ep2;
        this.overlap = 0;
        this.requests = 0;
    }

    public OverlapInfo(String ep1, String ep2, double overlap, int requests) {
        this.ep1 = ep1;
        this.ep2 = ep2;
        this.overlap = overlap;
        this.requests = requests;
    }

    /*******************************************************************************************************************
     * Public Methods
     ******************************************************************************************************************/

    public String getEp1() {
        return ep1;
    }

    public String getEp2() {
        return ep2;
    }

    public double getOverlap() {
        return overlap;
    }

    public int getRequests() {
        return requests;
    }

    public void setOverlap(double overlap) {
        this.overlap = overlap;
    }

    public void setRequests(int requests) {
        this.requests = requests;
    }

    public String toString(){
        return "{" + getEp1()+ ", " + getEp2() + ", " + getOverlap() + ", " + getRequests() + "}";
    }
}
