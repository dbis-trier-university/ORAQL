package QueryManagement.SourceSelection;

import Configuration.ConfigurationManagement;
import QueryManagement.Indexer.Index.OverlapIndex.OverlapIndex;
import QueryManagement.Indexer.Index.OverlapIndex.OverlapInfo;
import QueryManagement.Indexer.Indexer;
import QueryManagement.Indexer.Overlap.OverlapStack;
import QueryManagement.Processor.QueryPlan;
import QueryManagement.Utils.Endpoint;
import QueryManagement.Utils.ProbingResult;
import org.apache.jena.atlas.lib.Pair;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.vocabulary.RDF;

import java.util.*;

public class CompletenessEstimator {

    private Indexer indexer;

    private Map<String,String> entityClassMap;

    /*******************************************************************************************************************
     * Constructor
     ******************************************************************************************************************/

    public CompletenessEstimator(Indexer indexer) {
        this.indexer = indexer;
    }

    /*******************************************************************************************************************
     * Public Methods
     ******************************************************************************************************************/

    public void start(List<ProbingResult> probingResults, QueryPlan queryPlan){
        this.entityClassMap = createEntityClassMap(probingResults);

        for(ProbingResult result : probingResults){
            List<Endpoint> selectedEndpoints = remove(result);

            Map<Endpoint, Model> map = new HashMap<>();
            for(Endpoint ep : selectedEndpoints){
                map.put(ep,result.getResults(ep));
            }

            queryPlan.put(result.getTriple(),map);
        }
    }

    /*******************************************************************************************************************
     * Private Methods
     ******************************************************************************************************************/

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

    private List<Endpoint> remove(ProbingResult result){
        List<Endpoint> selection = result.getEndpoints();
        selection.sort(Comparator.comparingDouble(Endpoint::getReliability));

        OverlapStack overlapStack = new OverlapStack(result,indexer,entityClassMap);

        Iterator<Endpoint> it = selection.iterator();
        while (it.hasNext()){
            Endpoint ep = it.next();
            if(overlapStack.isCovered(ep)) it.remove();
        }

        return selection;
    }

}
