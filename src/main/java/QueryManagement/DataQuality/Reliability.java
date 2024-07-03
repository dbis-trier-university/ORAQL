package QueryManagement.DataQuality;

import QueryManagement.Processor.QueryPlan;
import QueryManagement.Utils.Endpoint;
import QueryManagement.Utils.Utils;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;

import java.util.*;

public class Reliability {

    /*******************************************************************************************************************
     * Public Methods
     ******************************************************************************************************************/

    public static double computeReliability(QueryPlan queryPlan){
        double reliability = 0;

        for(Map.Entry<Triple,Map<Endpoint, Model>> tripleEntry : queryPlan.entrySet()){
            double tpReliability = computeReliability(tripleEntry.getValue());
            if (reliability == 0) reliability = tpReliability;
            else if(tpReliability < reliability) reliability = tpReliability;
        }
//        reliability = reliability/queryPlan.numberOfTriples();

        return Utils.round(reliability);
    }



    public static double computeReliability(Map<Endpoint,Model> endpoints)
    {
        if(endpoints.size() > 2){
            return sumPoissonBinomialDistribution(endpoints);
        } else if (endpoints.size() == 2){
            double value = 0;

            for(Map.Entry<Endpoint,Model> entry : endpoints.entrySet()){
                if(entry.getKey().getReliability() > value) value = entry.getKey().getReliability();
            }

            return value;
        }
        else if (endpoints.size() == 1){
            return endpoints.entrySet().iterator().next().getKey().getReliability();
        } else {
            return 0; // TODO Implement an exception that tells the user that there is no data source for this triple
        }
    }

    /*******************************************************************************************************************
     * Private Methods
     ******************************************************************************************************************/

    private static double sumPoissonBinomialDistribution(Map<Endpoint,Model> endpoints) {
        double[] pk = sumPoissonBinomialDistribution(endpoints, Integer.MAX_VALUE,2);
        double result = 0;

        for (int i = ((int) Math.ceil(endpoints.size()/2.0)); i < pk.length; i++) {
            result += pk[i];
        }

        return result;
    }

    private static double[] sumPoissonBinomialDistribution(Map<Endpoint,Model> endpoints, int maxN, double maxCumPr)
    {
        List<Double> w = new LinkedList<>();
        for(Map.Entry<Endpoint,Model> entry : endpoints.entrySet()){
            double rel = entry.getKey().getReliability();
            w.add(rel/(1-rel));
        }
        w.sort(Double::compareTo);
        Collections.reverse(w);

        int n = endpoints.size();
        int mN = Math.min(maxN,n);

        double z = 1;
        for (Double d : w) {
            z = z / (d + 1.0);
        }

        double[] r = new double[n+1];
        Arrays.fill(r,1d);

        r[n] = z;

        int i = 1;
        int j = 0;
        int k = 0;
        int m = 0;
        double s = 0;
        var cumPr = r[n];

        while(cumPr < maxCumPr && i <= mN) {
            s = 0;
            j = 0;
            m = n - i;
            k = i - 1;

            while (j <= m) {
                s += r[j] * w.get(k + j);
                r[j] = s;
                j += 1;
            }

            r[j - 1] *= z;
            cumPr += r[j - 1];

            i += 1;
        }

        return finalizeR(r, i, n);
    }

    private static double[] finalizeR(double[] r, int i, int n){
//        if (i <= n) {
//            double[] smallerR = new double[i];
//            System.arraycopy(r,n - i + 1,smallerR,0,i);
//            return reverse(smallerR);
//        } else {
            return reverse(r);
//        }
    }

    private static double[] reverse(double[] a){
        double[] b = new double[a.length];
        int j = a.length;
        for (int i = 0; i < a.length; i++) {
            b[j - 1] = a[i];
            j = j - 1;
        }

        return b;
    }
}
