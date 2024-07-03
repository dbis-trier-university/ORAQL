package QueryManagement.Indexer.Index.OverlapIndex;

import QueryManagement.Utils.Endpoint;
import org.apache.jena.atlas.lib.Pair;

public class OverlapInfo {
    private Pair<Endpoint,Integer> ep1;
    private Pair<Endpoint,Integer> ep2;
    private double overlap;

    public OverlapInfo(Pair<Endpoint,Integer> ep1, Pair<Endpoint,Integer> ep2, double overlap) {
        this.ep1 = ep1;
        this.ep2 = ep2;
        this.overlap = overlap;
    }

    public Pair<Endpoint,Integer> getEp1() {
        return ep1;
    }

    public Pair<Endpoint,Integer> getEp2() {
        return ep2;
    }

    public double getOverlap() {
        return overlap;
    }

    @Override
    public String toString() {
        return "OverlapInfo{ " +
                "ep1='" + ep1 + '\'' +
                ", ep2='" + ep2 + '\'' +
                ", overlap=" + overlap +
                " }";
    }
}
