package QueryManagement.Indexer.Baseline;

import Configuration.ConfigurationManagement;
import QueryManagement.DataQuality.Reliability;
import QueryManagement.Indexer.Index.OverlapIndex.OverlapIndex;
import QueryManagement.Indexer.Indexer;
import QueryManagement.Processor.QueryPlan;
import QueryManagement.Utils.Endpoint;
import QueryManagement.Utils.ProbingResult;
import org.apache.jena.atlas.lib.Pair;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.vocabulary.RDF;

import java.util.*;

public class AcostaSelection {

    private Indexer indexer;

    private Map<String,String> entityClassMap;
    private Map<Triple,Integer> estSize;

    private double budget;
    private double minReliability;

    public AcostaSelection(Indexer indexer, double budget, double minReliability) {
        this.indexer = indexer;
        this.budget = budget;
        this.minReliability = minReliability;
        this.estSize = new HashMap<>();
    }

    public QueryPlan startSourceSelection(List<ProbingResult> probingResults)
    {
        // Initial reliability calculation
        QueryPlan queryPlan = new QueryPlan();

        this.entityClassMap = createEntityClassMap(probingResults);
        tripleEstimation(probingResults);

        double tripleBudget = this.budget/probingResults.size();
        for(ProbingResult result : probingResults){
            List<Endpoint> solution = utilityAwareOptimization(result,tripleBudget);

            Map<Endpoint,Model> selectedEndpoints = new HashMap<>();
            for(Endpoint ep : solution){
                selectedEndpoints.put(ep,result.getResults(ep));
            }

            queryPlan.put(result.getTriple(),selectedEndpoints);
        }

        return queryPlan;
    }

    /*******************************************************************************************************************
     * Original Acosta Algorithm
     ******************************************************************************************************************/

    private List<Endpoint> utilityAwareOptimization(ProbingResult result, double tripleBudget){
        List<List<Endpoint>> combinations = createSuperSet(result.getEndpoints());
        List<Endpoint> possibleEndpoints = result.getEndpoints();
        possibleEndpoints.sort(Comparator.comparing(Endpoint::getLabel));

        // Because of empty set subtract 1
        if(combinations.size()-1 <= tripleBudget){
            List<Endpoint> solution = result.getEndpoints().stream()
                    .filter((e1 -> e1.getReliability()-this.minReliability > 0)).toList(); // make it feasible

            if(!solution.isEmpty()){
                for(List<Endpoint> combination : combinations){
                    if(phi(result,combination) < phi(result,solution) && isFeasible(combination))
                        solution = combination;
                }
            }

            return solution;
        } else {
            int counter = 0;
            List<Endpoint> solution = result.getEndpoints().stream()
                    .filter((e1 -> e1.getReliability()-this.minReliability > 0)).toList(); // make it feasible

            while (counter < tripleBudget){
                List<Endpoint> newSolution = mutate(combinations);

                if(phi(result,newSolution) < phi(result,solution) && isFeasible(newSolution))
                    solution = newSolution;

                counter++;
            }

            return solution;
        }
    }

    public double phi(ProbingResult result, List<Endpoint> solution){
        double untrust = 1-computeReliability(solution);

        List<Pair<Endpoint,Integer>> solutionSizes = result.getAllEndpointResults().stream()
                .filter(e -> solution.contains(e.getLeft())).toList();

        double estSolutionSize = estimateSolutionSize(result,solutionSizes);
        double rel = estSolutionSize/this.estSize.get(result.getTriple());
        double missing = 1 - (rel > 1 ? 1 : rel);

        return (1.0/3.0 * untrust) + (2.0/3.0 * missing);
    }

    public boolean isFeasible(List<Endpoint> solution){
        for(Endpoint ep : solution){
            double untrust = 1 - ep.getReliability();
            double maxUntrust = 1 - this.minReliability;

            if(untrust- maxUntrust > 0) return false;
        }

        return true;
    }

    public double computeReliability(List<Endpoint> endpoints){
        Map<Endpoint, Model> tmp = new HashMap<>();

        for(Endpoint ep : endpoints){
            tmp.put(ep, ModelFactory.createDefaultModel());
        }

        return Reliability.computeReliability(tmp);
    }

    private List<Endpoint> mutate(List<List<Endpoint>> combinations){
        Random rand = new Random();
        int randomNum = rand.nextInt(((combinations.size()-1) - 1) + 1) + 1;

        return combinations.get(randomNum);
    }

    public static List<List<Endpoint>> createSuperSet(List<Endpoint> endpointList) {
        List<List<Endpoint>> superSet = new ArrayList<>();
        superSet.add(new ArrayList<>());

        for (Endpoint ep : endpointList) {
            int superSetSize = superSet.size();
            for (int i = 0; i < superSetSize; i++) {
                List<Endpoint> subset = new ArrayList<>(superSet.get(i));
                subset.add(ep);
                superSet.add(subset);
            }
        }

        superSet.remove(superSet.get(0));

        return superSet;
    }

    /*******************************************************************************************************************
     * Completeness Estimation
     ******************************************************************************************************************/

    private long estimateSolutionSize(ProbingResult result, List<Pair<Endpoint,Integer>> endpointResults) {
        long counter = endpointResults.get(0).getRight();

        for (int i = 0; i < endpointResults.size(); i++) {
            for (int j = i+1; j < endpointResults.size(); j++) {
                Endpoint epi = endpointResults.get(i).getLeft();
                Endpoint epj = endpointResults.get(j).getLeft();

                OverlapIndex oIndex = this.indexer.getEpIndex(epi.getLabel()).getEpIndex(epj);
                String type = this.entityClassMap.get(result.getTriple().getSubject().toString());
                String predicate = result.getTriple().getPredicate().toString();

                if(oIndex.getPredicateOccurrence(type,predicate) <= ConfigurationManagement.getMinIndexOccurrence()){
                    if(counter < this.estSize.get(result.getTriple())){
                        counter += endpointResults.get(j).getRight();
                    } else {
                        return counter;
                    }
                } else {
                    if(counter < this.estSize.get(result.getTriple())){
                        double overlap = oIndex.getPredicateOverlap(type,predicate);

                        if(endpointResults.get(i).getRight() > endpointResults.get(j).getRight()){
                            counter += Math.round(endpointResults.get(i).getRight() * (1-overlap));
                        } else {
                            counter += Math.round(endpointResults.get(j).getRight() * (1-overlap));
                        }
                    } else {
                        return counter;
                    }
                }
            }
        }

        return counter;
    }

    private void tripleEstimation(List<ProbingResult> probingResults){
        for(ProbingResult result : probingResults){
            this.estSize.put(result.getTriple(),estimate(result));
        }
    }

    private Map<String,String> createEntityClassMap(List<ProbingResult> probingResults){
        Map<String,String> entityClassMap = new HashMap<>();

        for(ProbingResult result : probingResults){
            Triple triple = result.getTriple();

            if(triple.getPredicate().toString().equals(RDF.type.toString())){
                if(triple.getObject().isVariable()){
                    entityClassMap.put(triple.getSubject().toString(),null);
                } else {
                    entityClassMap.put(triple.getSubject().toString(),triple.getObject().toString());
                }

            }
        }

        return entityClassMap;
    }

    private int estimate(ProbingResult result){
        double estimation = 0;
        int counter = 0;

        List<Pair<Endpoint,Integer>> endpointResults = result.getAllEndpointResults();
        for(Pair<Endpoint,Integer> pair1 : endpointResults){
            for(Pair<Endpoint,Integer> pair2 : endpointResults){
                if(!pair1.getLeft().getLabel().equalsIgnoreCase(pair2.getLeft().getLabel())){
                    OverlapIndex oIndex = this.indexer.getEpIndex(pair1.getLeft().getLabel()).getEpIndex(pair2.getLeft());

                    String type = this.entityClassMap.get(result.getTriple().getSubject().toString());
                    String predicate = result.getTriple().getPredicate().toString();

                    if(oIndex.getPredicateOccurrence(type,predicate) <= ConfigurationManagement.getMinIndexOccurrence()){
                        estimation += pair1.getRight() + pair2.getRight();
                    } else {
                        double overlap = oIndex.getPredicateOverlap(type,predicate);
                        estimation += captureRecapture(pair1.getRight(), pair2.getRight(), overlap);
                    }

                    counter++;
                }
            }
        }
        estimation = estimation/counter;

        return (int)(Math.round(estimation));
    }

    private double captureRecapture(int n, int m, double overlap){
        double k = n < m ? m * overlap : n * overlap;

        return ((n*m)/k);
    }
}
