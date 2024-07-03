package QueryManagement.SourceSelection;

import Configuration.UserPreferences;
import QueryManagement.DataQuality.Reliability;
import QueryManagement.Indexer.Indexer;
import QueryManagement.Processor.QueryPlan;
import QueryManagement.Utils.Endpoint;
import QueryManagement.Utils.ProbingResult;
import org.apache.jena.atlas.lib.Pair;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.Model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SourceSelection {

    /*******************************************************************************************************************
     * Public Methods
     ******************************************************************************************************************/
    public static QueryPlan startSourceSelection(Indexer index,
                                                 List<ProbingResult> probingResults,
                                                 UserPreferences preferences)
    {
        // Initial reliability calculation
        QueryPlan queryPlan = new QueryPlan();

        // Select sources to get a complete query result
        CompletenessEstimator compEstimator = new CompletenessEstimator(index);
        compEstimator.start(probingResults,queryPlan);

        // Use more endpoints to increase reliability
        ReliabilityEstimator relEstimator = new ReliabilityEstimator(queryPlan,probingResults,preferences);
        relEstimator.start();

        return queryPlan;
    }

}
