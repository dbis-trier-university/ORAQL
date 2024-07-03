package QueryManagement.Indexer.Overlap;

import Configuration.ConfigurationManagement;
import QueryManagement.DataCrawler.Transformer;
import QueryManagement.Indexer.Index.EpIndex;
import QueryManagement.Indexer.Index.OverlapIndex.OverlapIndex;
import QueryManagement.Indexer.TripleFactory;
import QueryManagement.Processor.MetaData;
import QueryManagement.Requests.HttpHandler;
import QueryManagement.Requests.HttpResponse;
import QueryManagement.Utils.Endpoint;
import org.apache.jena.atlas.lib.Pair;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Statement;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

public class OverlapFactory {
    private boolean additional;
    private List<EpIndex> indexList;
    private int noOfEntities;

    /*******************************************************************************************************************
     * Constructor
     ******************************************************************************************************************/

    public OverlapFactory(List<EpIndex> indexList) {
        this.indexList = indexList;
        this.noOfEntities = ConfigurationManagement.getEntitiesSize();
        this.additional = ConfigurationManagement.additionalCrawling();
    }

    /*******************************************************************************************************************
     * Public Methods
     ******************************************************************************************************************/

    public void createOverlapIndexes(){
        for (int i = 0; i < this.indexList.size(); i++) {
            for (int j = 0; j < this.indexList.size(); j++) {
                if(i != j){
                    System.out.println("Overlap: (" + indexList.get(i).getEndpoint().getLabel() + "," + indexList.get(j).getEndpoint().getLabel() + ")");
                    OverlapIndex overlapIndex = createOverlapIndex(i,j);

                    indexList.get(i).addOverlapIndex(indexList.get(j).getEndpoint(),overlapIndex);

                    OverlapWriter.write(indexList.get(i).getEndpoint(), indexList.get(j).getEndpoint(), overlapIndex);
                }
            }
        }
    }

    /*******************************************************************************************************************
     * Private Methods
     ******************************************************************************************************************/

    private OverlapIndex createOverlapIndex(int i, int j){
        OverlapIndex overlapIndex = new OverlapIndex();

        Map<String,Set<String>> ep1TypeEntityMap = this.indexList.get(i).getEntitiesTypeMap();
        Model ep1Model = this.indexList.get(i).getSampleModel();
        Model ep2Model = this.indexList.get(j).getSampleModel();

        for(Map.Entry<String,Set<String>> typeEntry : ep1TypeEntityMap.entrySet()){
            System.out.println(typeEntry.getKey());
            Set<String> ep2Classes = indexList.get(j).getCharSet().getClasses();

            // Generic types should be ignored
            if(ep2Classes.contains(typeEntry.getKey()) && !TypeChecker.isGenericType(typeEntry.getKey())){
                int counter = 0;

                Map<String, Pair<Double,Integer>> predicateMap = getInnerMap(overlapIndex, typeEntry.getKey());
                List<String> remainderEntities = computeOverlap(ep1Model,ep2Model,typeEntry.getValue(),predicateMap, counter);

                if(additional){
                    System.out.println("Start to Request Additional Entities");
                    Endpoint ep = indexList.get(j).getEndpoint();
                    requestAdditional(ep,ep1Model,remainderEntities,predicateMap,counter);
                }

                overlapIndex.put(typeEntry.getKey(),predicateMap);
            }
        }

        return overlapIndex;
    }

    private Map<String, Pair<Double,Integer>> getInnerMap(OverlapIndex overlapIndex, String type)
    {
        Map<String, Pair<Double,Integer>> relationsMap = new HashMap<>();

        if(overlapIndex.containsKey(type)){
            relationsMap = overlapIndex.get(type);
        }

        return relationsMap;
    }

    private List<String> computeOverlap(Model ep1Model,
                                           Model ep2Model,
                                           Set<String> entities,
                                           Map<String, Pair<Double,Integer>> predicateCountOccurenceMap,
                                           int counter)
    {
        List<String> remainderEntities = new LinkedList<>();

        for(String entity : entities){
            if(noOfEntities != -1 && counter >= noOfEntities) break;

            List<Statement> ep1Statements = TripleFactory.querySubjectTriples(entity,ep1Model);
            List<Statement> ep2Statements = TripleFactory.querySubjectTriples(entity,ep2Model);

            if(!ep2Statements.isEmpty()){
                OverlapComputation.compute(ep1Statements,ep2Statements,predicateCountOccurenceMap);
                counter++;
            } else {
                remainderEntities.add(entity);
            }

        }

        return remainderEntities;
    }

    private void requestAdditional(Endpoint ep,
                                          Model ep1Model,
                                          List<String> remainderEntities,
                                          Map<String, Pair<Double,Integer>> predicateCountOccurenceMap,
                                          int counter)
    {
        for (int k = 0; counter < noOfEntities && k < remainderEntities.size(); k++) {
            System.out.println("Request Additional: " + counter);

            String entity = remainderEntities.get(k);
            Model ep2Model = requestEntity(ep,entity);
            List<Statement> ep1Statements = TripleFactory.querySubjectTriples(entity,ep1Model);

            if(ep2Model != null) {
                List<Statement> ep2Statements = TripleFactory.querySubjectTriples(entity,ep2Model);
                OverlapComputation.compute(ep1Statements,ep2Statements,predicateCountOccurenceMap);
            } else {
                for(Statement ep1Statement : ep1Statements) {
                    Property p1 = ep1Statement.getPredicate();
                    RDFNode o1 = ep1Statement.getObject();

                    if(!o1.isAnon()){
                        if(predicateCountOccurenceMap.containsKey(p1.toString())){
                            predicateCountOccurenceMap.put(p1.toString(),new Pair<>(predicateCountOccurenceMap.get(p1.toString()).getLeft(),predicateCountOccurenceMap.get(p1.toString()).getRight() + 1));
                        } else {
                            predicateCountOccurenceMap.put(p1.toString(),new Pair<>(0.0,1));
                        }
                    }
                }
            }

            counter++;
        }
    }

    private Model requestEntity(Endpoint ep, String entity){
        String url = ep.getUrl();
        url = url.replace("{s}",entity);
        url = url.replace("{p}","");
        url = url.replace("{o}","");

        HttpResponse response = HttpHandler.sendGetRequest(url,ep.getTime(),0);
        if(response != null){
            Model model = Transformer.transformToModel(response);
            Model metaModel = MetaData.removeMetaData(model);

            return model;
        }

        return null;
    }

}
