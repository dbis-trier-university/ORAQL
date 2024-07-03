package QueryManagement.SourceSelection;

import Configuration.UserPreferences;
import QueryManagement.DataQuality.Reliability;
import QueryManagement.Processor.QueryPlan;
import QueryManagement.Utils.Endpoint;
import QueryManagement.Utils.ProbingResult;
import org.apache.jena.atlas.lib.Pair;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.Model;

import java.util.*;

public class ReliabilityEstimator {

    private QueryPlan queryPlan;
    private List<ProbingResult> probingResults;

    private UserPreferences preferences;

    private Map<Triple,Boolean> checker;

    /*******************************************************************************************************************
     * Constructor
     ******************************************************************************************************************/

    public ReliabilityEstimator(QueryPlan queryPlan, List<ProbingResult> probingResults, UserPreferences preferences) {
        this.queryPlan = queryPlan;
        this.probingResults = probingResults;
        this.preferences = preferences;
        this.checker = new HashMap<>();
    }

    /*******************************************************************************************************************
     * Public Methods
     ******************************************************************************************************************/

    public void start() {
        double rel = Reliability.computeReliability(queryPlan);

        for (int i = 0; rel < preferences.getMinReliability() ; i = (i + 1) % probingResults.size()) {
            // Helper variables
            Triple triple = probingResults.get(i).getTriple();
            Map<Endpoint, Model> selectedEndpoints = queryPlan.getEndpoints(triple);
            List<Pair<Endpoint,Integer>> possibleEndpoints = probingResults.get(i).getAllEndpointResults();

            // Remove endpoints with no results and order them by reliability
            possibleEndpoints.removeIf(e -> selectedEndpoints.containsKey(e.getLeft()) || e.getRight() == 0);
            possibleEndpoints.sort(Comparator.comparingDouble(e -> e.getLeft().getReliability()));
            Collections.reverse(possibleEndpoints);

            // Add for each triple an endpoint to ensure enough reliability
            increaseTripleReliability(triple,selectedEndpoints,possibleEndpoints,preferences);

            // Add new selected endpoint to the query plan
            queryPlan.put(triple,selectedEndpoints);
            rel = Reliability.computeReliability(queryPlan);

            // Check if all endpoints are used. If yes, stop the improvement
            if(allEndpointsUsed()) break;
        }

        System.out.println("Reliability: " + rel);
    }

    /*******************************************************************************************************************
     * Private Methods
     ******************************************************************************************************************/

    private void increaseTripleReliability(Triple triple,
                                                  Map<Endpoint,Model> selectedEndpoints,
                                                  List<Pair<Endpoint,Integer>> possibleEndpoints,
                                                  UserPreferences preferences)
    {
        double tripleRel = Reliability.computeReliability(selectedEndpoints);

        if(tripleRel < preferences.getMinReliability()){
            if(!possibleEndpoints.isEmpty() && possibleEndpoints.get(0).getRight() > 0) {
                Endpoint ep = possibleEndpoints.get(0).getLeft();
                Model model = ProbingResult.search(triple,ep,probingResults);
                selectedEndpoints.put(ep,model);
            } else if(possibleEndpoints.isEmpty() || possibleEndpoints.get(0).getRight() <= 0) {
                this.checker.put(triple,true);
            }
        } else {
            this.checker.put(triple,true);
        }
    }

    private boolean allEndpointsUsed() {
        boolean b = true;

        for(ProbingResult probingResult : this.probingResults){
            try{
                b = b & this.checker.get(probingResult.getTriple());
            } catch (NullPointerException e){
                return false;
            }
        }

        return b;
    }
}
