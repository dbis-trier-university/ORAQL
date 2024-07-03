package QueryManagement.DataCrawler;

import QueryManagement.DataCrawler.JoinStrategies.JoinProcessor;
import QueryManagement.Processor.QueryPlan;
import QueryManagement.Utils.Endpoint;
import QueryManagement.Utils.ProbingResult;
import org.apache.jena.rdf.model.*;

import java.util.List;
import java.util.Map;

public class Crawler {

    /*******************************************************************************************************************
     * Public Methods
     ******************************************************************************************************************/

    public static Model executeQueryPlan(QueryPlan queryPlan, List<ProbingResult> probingResults){
        Model finalModel = ModelFactory.createDefaultModel();

        for (int i = 0; i < probingResults.size(); i++) {
            Map<Endpoint, Model> tripleResult;
            ProbingResult probingResult = probingResults.get(i);

            JoinProcessor jp = new JoinProcessor(probingResult,queryPlan);
            tripleResult = jp.fullJoin();

            Model mergedModel = ModelCreator.createMergedModel(tripleResult,probingResult.getTriple()); //
            finalModel.add(mergedModel);
        }

        return finalModel;
    }

    public static Model executeAcostaPlan(QueryPlan queryPlan, List<ProbingResult> probingResults){
        long lastTriples;
        Model finalModel = ModelFactory.createDefaultModel();

        for (int i = 0; i < probingResults.size(); i++) {
            Map<Endpoint, Model> tripleResult;
            ProbingResult probingResult = probingResults.get(i);

            JoinProcessor jp = new JoinProcessor(probingResult,queryPlan);
            tripleResult = jp.fullJoin();

            Model mergedModel = ModelCreator.createMergedModel(tripleResult);
            finalModel.add(mergedModel);
            lastTriples = mergedModel.size();

            System.out.println(probingResult.getTriple() + ": " + lastTriples);
        }

        return finalModel;
    }

    /*******************************************************************************************************************
     * Private Methods
     ******************************************************************************************************************/

    private static long getEntities(int i,List<ProbingResult> probingResults, Model intermediateModel){
        if(i == 0){
            return probingResults.get(i).getMaxTriples();
        } else {
            if(isObjectNeededNext(i,probingResults)){
                return intermediateModel.listObjects().toList().size();
            } else {
                return intermediateModel.listSubjects().toList().size();
            }
        }
    }

    public static boolean isObjectNeededNext(int i, List<ProbingResult> probingResults){
        if(i == probingResults.size() - 1) return false;
        else {
            return probingResults.get(i+1).getTriple().getSubject()
                    .equals(probingResults.get(i).getTriple().getObject());
        }
    }

    public static int isObjectNeeded(int i, List<ProbingResult> probingResults){
        if(i == 0) return 0;
        else if (probingResults.get(i).getTriple().getSubject()
                .equals(probingResults.get(i-1).getTriple().getObject()))
        {
            return -1;
        }
        else if(probingResults.get(i).getTriple().getObject()
                .equals(probingResults.get(i-1).getTriple().getSubject())){
            return 1;
        }

        return 0;
    }

}
