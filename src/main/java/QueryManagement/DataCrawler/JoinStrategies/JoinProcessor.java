package QueryManagement.DataCrawler.JoinStrategies;

import QueryManagement.DataCrawler.Transformer;
import QueryManagement.DataCrawler.UrlCreator;
import QueryManagement.Processor.MetaData;
import QueryManagement.Processor.QueryPlan;
import QueryManagement.Requests.HttpHandler;
import QueryManagement.Requests.HttpResponse;
import QueryManagement.Utils.Endpoint;
import QueryManagement.Utils.ProbingResult;
import org.apache.jena.arq.querybuilder.ConstructBuilder;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.rdf.model.*;

import java.util.Iterator;
import java.util.Map;

public class JoinProcessor {

    private ProbingResult probingResult;
    private QueryPlan queryPlan;

    /*******************************************************************************************************************
     * Constructor
     ******************************************************************************************************************/

    public JoinProcessor(ProbingResult probingResult, QueryPlan queryPlan) {
        this.probingResult = probingResult;
        this.queryPlan = queryPlan;
    }

    /*******************************************************************************************************************
     * Public Methods
     ******************************************************************************************************************/

    public Map<Endpoint, Model> fullJoin(){
        // System.out.println("Full Join: " + probingResult.getTriple());
        Map<Endpoint, Model> endpointResults = queryPlan.getEndpoints(probingResult.getTriple());

        for(Map.Entry<Endpoint,Model> endpointResult : endpointResults.entrySet()){
            Model metaModel = MetaData.removeMetaData(endpointResult.getValue());
            String url = MetaData.nextPageUrl(metaModel);
            HttpResponse response = HttpHandler.sendGetRequest(url,endpointResult.getKey().getTime(),0);
            long resultSize = MetaData.getResultSize(metaModel);

            crawlAllPages(endpointResult.getKey(),resultSize,response);
        }

        return endpointResults;
    }

    public static Map<Endpoint, Model> loopJoin(ProbingResult probingResult, Model intermediateModel, QueryPlan queryPlan, int bindingCase, Triple lastTriple) {
        System.out.println("Loop Join: " + probingResult.getTriple());
        Map<Endpoint, Model> endpointResults = queryPlan.getEndpoints(probingResult.getTriple());

        for(Map.Entry<Endpoint,Model> endpointResult : endpointResults.entrySet()){
            Model finalModel = ModelFactory.createDefaultModel();

            Iterator it;
            Model filteredModel = filterModel(intermediateModel,lastTriple);
            if(bindingCase == -1) it = filteredModel.listObjects();
            else it = filteredModel.listSubjects();

            String baseUrl = endpointResult.getKey().getUrl();

            while (it.hasNext()){
                Object o = it.next();
                String url;

                if(o instanceof Resource){
                    if(bindingCase == -1) {
                        url = UrlCreator.prepareSubjectUrl(baseUrl,probingResult.getTriple(),((Resource) o).getURI());
                    } else if (bindingCase == 1){
                        url = UrlCreator.prepareObjectUrl(baseUrl,probingResult.getTriple(),((Resource) o).getURI());
                    } else {
                        url = UrlCreator.prepareSubjectUrl(baseUrl,probingResult.getTriple(),((Resource) o).getURI());
                    }

                    Model model;
                    Model metaModel = null;

                    do {
                        HttpResponse response = HttpHandler.sendGetRequest(url,endpointResult.getKey().getTime(),0);

                        if(response != null){
                            model = Transformer.transformToModel(response);
                            metaModel = MetaData.removeMetaData(model);
                            queryPlan.update(probingResult.getTriple(),endpointResult.getKey(),model);
                            finalModel.add(model);

                            url = MetaData.nextPageUrl(metaModel);
                        }
                    } while (metaModel != null && MetaData.hasNextPage(metaModel));
                }
            }

            endpointResults.put(endpointResult.getKey(),finalModel);
        }

        return endpointResults;
    }

    /*******************************************************************************************************************
     * Private Methods
     ******************************************************************************************************************/

    private void crawlAllPages(Endpoint ep, long resultSize, HttpResponse response){
        long modelSize = 0;

        while (modelSize < resultSize){
            Model model = Transformer.transformToModel(response);
            Model metaModel = MetaData.removeMetaData(model);
            queryPlan.update(probingResult.getTriple(),ep,model);
            modelSize = queryPlan.getEndpoints(probingResult.getTriple()).get(ep).size();

            String url = MetaData.nextPageUrl(metaModel);
            response = HttpHandler.sendGetRequest(url,ep.getTime(),0);
            while (response == null) response = HttpHandler.sendGetRequest(url,ep.getTime(),0);
        }
    }

    private static Model filterModel(Model model, Triple triple){
        ConstructBuilder cb = new ConstructBuilder().addConstruct(triple).addWhere(triple);

        Query q = cb.build();
        QueryExecution qExec = QueryExecutionFactory.create(q,model);
        Model filteredModel = qExec.execConstruct();

        return filteredModel;
    }

}
