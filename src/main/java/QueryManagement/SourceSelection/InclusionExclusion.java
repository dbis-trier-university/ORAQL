package QueryManagement.SourceSelection;

import QueryManagement.Indexer.Index.OverlapIndex.OverlapIndex;
import QueryManagement.Indexer.Index.OverlapIndex.OverlapInfo;
import QueryManagement.Indexer.Indexer;
import QueryManagement.Utils.Endpoint;
import QueryManagement.Utils.ProbingResult;
import org.apache.commons.math3.util.CombinatoricsUtils;
import org.apache.jena.atlas.lib.Pair;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class InclusionExclusion {

    public static int computeSize(ProbingResult result, Indexer indexer, Map<String,String> entityClassMap, List<Endpoint> selection){
        return ((int) Math.ceil(union(result,indexer,entityClassMap,selection)));
    }

    private static double union(ProbingResult result, Indexer indexer, Map<String,String> entityClassMap, List<Endpoint> selection){
        double value = 0;

        for (int k = 1; k <= selection.size();k++) {
            double tmp = Math.pow(-1,k-1) * intersect(result,indexer,entityClassMap,selection,k);
            value += tmp;
        }

        return value;
    }

    private static double intersect(ProbingResult result, Indexer indexer, Map<String,String> entityClassMap, List<Endpoint> selection, int k){
        String type = entityClassMap.get(result.getTriple().getSubject().toString());
        String predicate = result.getTriple().getPredicate().toString();

        double value = 0;

        Iterator<int[]> iterator = CombinatoricsUtils.combinationsIterator(selection.size(),k);
        while (iterator.hasNext()){
            final int[] combination = iterator.next();

            if(k == 1){
                value += result.getEndpointResults(selection.get(combination[0]));
            } else if (k == 2) {
                value += computeTupleCombinations(result,indexer,type,predicate,selection,combination);
            }
        }

        return value;
    }

    private static double computeTupleCombinations(ProbingResult result, Indexer indexer,String type, String predicate, List<Endpoint> selection, int[] combination){
        double value = 0;

        Iterator<int[]> iterator = CombinatoricsUtils.combinationsIterator(combination.length,2);
        while (iterator.hasNext()){
            final int[] tuple = iterator.next();

            for (int i = 0; i < tuple.length; i++) {
                for (int j = i+1; j < tuple.length; j++) {
                    String iLabel = selection.get(combination[tuple[i]]).getLabel();
                    Endpoint jEndpoint = selection.get(combination[tuple[j]]);
                    OverlapIndex overlap = indexer.getEpIndex(iLabel).getEpIndex(jEndpoint);
                    try{
                        value += result.getEndpointResults(selection.get(combination[tuple[i]])) * overlap.getPredicateOverlap(type,predicate);
                    } catch (Exception e){
                        System.out.println();
                    }
                }
            }
//            for(int i : tuple){
//                for(int j : tuple){
//                    if(i != j){
//                        String iLabel = selection.get(combination[i]).getLabel();
//                        Endpoint jEndpoint = selection.get(combination[j]);
//                        OverlapIndex overlap = indexer.getEpIndex(iLabel).getEpIndex(jEndpoint);
//                        try{
//                            value += result.getEndpointResults(selection.get(combination[i])) * overlap.getPredicateOverlap(type,predicate);
//                        } catch (Exception e){
//                            System.out.println();
//                        }
//                    }
//                }
//            }
        }

        return value;
    }

}
