package QueryManagement.Utils;

import QueryManagement.Requests.HttpResponse;
import org.apache.jena.atlas.lib.Pair;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.Model;

import java.util.*;
import java.util.concurrent.Callable;

public class ProbingResult {
    private Triple triple;
    private List<Pair<Endpoint,Integer>> endpointResults; // TODO Change to map
    private Map<Endpoint,Model> intermediateResults;
    private Map<Endpoint,Long> neededCalls;
    private Callable<List<Pair<Endpoint,HttpResponse>>> callables;

    /*******************************************************************************************************************
     * Constructor
     ******************************************************************************************************************/

    public ProbingResult(Triple triple) {
        this.triple = triple;
        this.endpointResults = new LinkedList<>();
        this.intermediateResults = new HashMap<>();
        this.neededCalls = new HashMap<>();
    }

    /*******************************************************************************************************************
     * Public Methods
     ******************************************************************************************************************/

    public void addResult(Endpoint endpoint, int resultSize, long pages){
        this.endpointResults.add(new Pair<>(endpoint,resultSize));
        this.endpointResults.sort(Comparator.comparing(Pair::getRight));
        Collections.reverse(this.endpointResults);

        this.neededCalls.put(endpoint,pages);
    }

    public void addResult(Endpoint endpoint, Model model, int resultSize, long pages){
        addResult(endpoint,resultSize,pages);
        this.intermediateResults.put(endpoint,model);
    }

    public void setCallables(Callable<List<Pair<Endpoint,HttpResponse>>> c){
        this.callables = c;
    }

    public Callable<List<Pair<Endpoint, HttpResponse>>> getCallables() {
        return callables;
    }

    public Triple getTriple() {
        return triple;
    }

    public String getTripleSubject(){
        return this.triple.getSubject().toString();
    }

    public long getMaxCalls(){
        long calls = -1;

        for(Map.Entry<Endpoint,Long> entry : this.neededCalls.entrySet()){
            if(calls < entry.getValue()) calls = entry.getValue();
        }

        return calls;
    }

    public long getMaxTriples(){
        long triples = -1;

        for(Map.Entry<Endpoint,Model> entry : this.intermediateResults.entrySet()){
            if(triples < entry.getValue().size()) triples = entry.getValue().size();
        }

        return triples;
    }

    public List<Endpoint> getEndpoints(){
        List<Endpoint> list = new LinkedList<>();

        for(Map.Entry<Endpoint,Long> entry : this.neededCalls.entrySet()) list.add(entry.getKey());

        return list;
    }

    public List<Pair<Endpoint, Integer>> getAllEndpointResults() {
        return endpointResults;
    }

    public int getEndpointResults(Endpoint ep){
        for(Pair<Endpoint,Integer> pair : this.endpointResults){
            if(pair.getLeft().equals(ep)) return pair.getRight();
        }

        return -1;
    }

    public Map<Endpoint, Model> getIntermediateResults() {
        return intermediateResults;
    }

    public Model getResults(Endpoint endpoint){
        for(Map.Entry<Endpoint,Model> entry : this.intermediateResults.entrySet()){
            if(entry.getKey().getLabel().equalsIgnoreCase(endpoint.getLabel())){
                return entry.getValue();
            }
        }

        return null;
    }

    public void removeEndpoint(Pair<Endpoint,Integer> pair){
        this.endpointResults.remove(pair);
    }

    public String toString(){
        StringBuilder str = new StringBuilder(triple.toString() + ": ");

        for(Pair<Endpoint,Integer> pair : endpointResults){
            str.append("(").append(pair.getLeft()).append(", t:").append(pair.getRight())
                    .append(", p:").append(neededCalls.get(pair.getLeft())).append(")");
        }

        return str.toString();
    }

    /*******************************************************************************************************************
     * Static Methods
     ******************************************************************************************************************/

    public static Model search(Triple triple, Endpoint ep, List<ProbingResult> probingResults){
        for(ProbingResult probingResult : probingResults){
            if(probingResult.getTriple().equals(triple)){
                return probingResult.getIntermediateResults().get(ep);
            }
        }

        return null;
    }

}
