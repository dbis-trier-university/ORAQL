package QueryManagement.DataCrawler;

import QueryManagement.Indexer.Index.EpIndex;
import QueryManagement.Indexer.Indexer;
import QueryManagement.Processor.MetaData;
import QueryManagement.Requests.HttpHandler;
import QueryManagement.Requests.HttpResponse;
import QueryManagement.Utils.Endpoint;
import QueryManagement.Utils.Federation;
import QueryManagement.Utils.ProbingResult;
import org.apache.jena.atlas.lib.Pair;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

public class Probing {
    /*******************************************************************************************************************
     * Public Methods
     ******************************************************************************************************************/
    public static List<ProbingResult> start(Indexer indexer, List<Triple> tripleList, Federation federation){
        List<Endpoint> endpointList = federation.getMembers();

        List<ProbingResult> threads = schedule(tripleList,endpointList);
        List<ProbingResult> responses = execute(threads);
        filter(responses);

        return order(responses);
    }

    /*******************************************************************************************************************
     * Private Methods
     ******************************************************************************************************************/

    private static List<ProbingResult> schedule(List<Triple> tripleList, List<Endpoint> endpointList){
        List<ProbingResult> probingResults = new LinkedList<>();

        for(Triple triple : tripleList){
            ProbingResult result = new ProbingResult(triple);

            Callable<List<Pair<Endpoint, HttpResponse>>> c = new Callable<>() {
                @Override
                public List<Pair<Endpoint,HttpResponse>> call() {
                    List<Pair<Endpoint,HttpResponse>> responses = new LinkedList<>();

                    for(Endpoint endpoint : endpointList){
                        String url = UrlCreator.prepareUrl(endpoint.getUrl(),triple);
                        responses.add(new Pair<>(endpoint, HttpHandler.sendGetRequest(url,endpoint.getTime(),0)));
                    }

                    return responses;
                }
            };
            result.setCallables(c);
            probingResults.add(result);
        }

        return probingResults;
    }

    private static List<ProbingResult> execute(List<ProbingResult> probingResults){

        for(ProbingResult probingResult : probingResults){
            try {
                List<Pair<Endpoint,HttpResponse>> list = probingResult.getCallables().call();
                for(Pair<Endpoint,HttpResponse> pair : list){
                    int size = determineResultSize(pair.getRight());

                    if(size != 0) {
                        Model model = Transformer.transformToModel(pair.getRight().getContent());
                        long pages = MetaData.getNumberOfPages(model,MetaData.getMetaModel(model));

                        probingResult.addResult(pair.getLeft(),model,size,pages);
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        if(probingResults.size() > 1) sort(probingResults);

        return probingResults;
    }

    private static int determineResultSize(HttpResponse response){
        if(response != null){
            Model model = ModelFactory.createDefaultModel();
            InputStream is = new ByteArrayInputStream(response.getContent().getBytes());
            model.read(is, null, "N-TRIPLES");

            String queryStr = "select * where { ?s <http://rdfs.org/ns/void#triples> ?no . }";
            Query query = QueryFactory.create(queryStr);
            try (QueryExecution qexec = QueryExecutionFactory.create(query, model)) {
                List<QuerySolution> solutions = ResultSetFormatter.toList(qexec.execSelect());
                return Integer.parseInt(solutions.get(0).getLiteral("no").toString());
            }
        }

        return 0; // TODO -1 as error indicator?
    }

    private static void sort(List<ProbingResult> probingResults){
        try{
            probingResults.sort(Comparator.comparing(r -> r.getAllEndpointResults().get(0).getRight()));
        } catch (Exception e){
            System.out.println("Sort Error: " + e.getMessage());
        }
    }

    private static void filter(List<ProbingResult> probingResults){
        for(ProbingResult probingResult : probingResults){
            List<Pair<Endpoint,Integer>> remove = new LinkedList<>();

            for(Pair<Endpoint,Integer> pair : probingResult.getAllEndpointResults()){
                if(pair.getRight() <= 0) remove.add(pair);
            }

            for(Pair<Endpoint,Integer> pair : remove){
                probingResult.removeEndpoint(pair);
            }
        }
    }

    private static List<ProbingResult> order(List<ProbingResult> probingResults){
        List<ProbingResult> orderedProbingResults = new LinkedList<>();

        while (!probingResults.isEmpty()){
            probingResults.sort(Comparator.comparing(ProbingResult::getMaxCalls));
            ProbingResult tmp = probingResults.get(0);

            if(!orderedProbingResults.isEmpty()){
                Triple triple = orderedProbingResults.get(orderedProbingResults.size()-1).getTriple();

                if(!tmp.getTriple().getObject().equals(triple.getObject())){
                    for(ProbingResult result : probingResults){
                        if(result.getTriple().getObject().equals(triple.getSubject())) {
                            tmp = result;
                            break;
                        }
                    }
                }
            }

            orderedProbingResults.add(tmp);
            probingResults.remove(tmp);

            ProbingResult finalTmp = tmp;
            List<ProbingResult> tmpList = new LinkedList<>(probingResults.stream()
                    .filter(p -> p.getTripleSubject().equals(finalTmp.getTripleSubject())).toList());
            tmpList.sort(Comparator.comparing(ProbingResult::getMaxCalls));

            orderedProbingResults.addAll(tmpList);
            probingResults.removeAll(tmpList);
        }

        return orderedProbingResults;
    }

    private static Set<String> collectQueryProperties(List<Triple> tripleList){
        Set<String> properties = new HashSet<>();

        for(Triple triple : tripleList){
            properties.add(triple.getPredicate().toString());
        }

        return properties;
    }
}
