package QueryManagement.Processor;

import QueryManagement.Utils.Endpoint;
import QueryManagement.Utils.ProbingResult;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.Model;

import java.util.*;

public class QueryPlan {
    private Map<Triple, Map<Endpoint, Model>> endpointSelection;

    /*******************************************************************************************************************
     * Constructor
     ******************************************************************************************************************/

    public QueryPlan() {
        this.endpointSelection = new HashMap<>();
    }

    /*******************************************************************************************************************
     * Public Methods
     ******************************************************************************************************************/

    public Map<Endpoint,Model> getEndpoints(Triple triple){
        return this.endpointSelection.get(triple);
    }

    public void put(Triple triple, Map<Endpoint,Model> selectedEndpoints){
        this.endpointSelection.put(triple,selectedEndpoints);
    }

    public void update(Triple triple, Endpoint endpoint, Model model){
        Map<Endpoint,Model> map = this.endpointSelection.get(triple);

        if(this.endpointSelection.get(triple).containsKey(endpoint)){
            Model oldModel = map.get(endpoint);
            oldModel.add(model);
            map.put(endpoint,oldModel);
        } else {
            map.put(endpoint,model);
        }

        this.endpointSelection.put(triple,map);
    }

    public Set<Map.Entry<Triple,Map<Endpoint,Model>>> entrySet(){
        return this.endpointSelection.entrySet();
    }
}
