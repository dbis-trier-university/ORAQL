package Evaluation;

import QueryManagement.Indexer.Index.OverlapIndex.OverlapIndex;
import QueryManagement.Indexer.Overlap.OverlapComputation;
import QueryManagement.Indexer.Overlap.OverlapWriter;
import org.apache.jena.atlas.lib.Pair;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.tdb2.TDB2Factory;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class OverlapEvaluation {

    public static void main(String[] args) {
        List<String> endpoints = init();

        for(String ep1 : endpoints){
            for(String ep2 : endpoints){
                if(!ep1.equals(ep2)){
                    OverlapIndex overlapIndex = new OverlapIndex();

                    String tdb1Path = "C:\\Databases\\sample_dbs\\"+ep1+"\\tdb";
                    Dataset tdb1 = TDB2Factory.connectDataset(tdb1Path);
                    Model model1 =tdb1.getDefaultModel();
                    System.out.println(tdb1Path);

                    String tdb2Path = "C:\\Databases\\sample_dbs\\"+ep2+"\\tdb";
                    Dataset tdb2 = TDB2Factory.connectDataset(tdb2Path);
                    Model model2 =tdb2.getDefaultModel();
                    System.out.println(tdb2Path);

                    String typeQuery = "select distinct ?type where { ?s a ?type . }";
                    QueryExecution typeExec = QueryExecutionFactory.create(typeQuery,model1);
                    tdb1.begin(ReadWrite.READ);
                    List<QuerySolution> types = ResultSetFormatter.toList(typeExec.execSelect());
                    typeExec.close();
                    tdb1.end();

                    System.out.println(types.size() + " types");
                    for(QuerySolution typeSolution : types){
                        RDFNode typeNode;
                        try{
                            typeNode = typeSolution.get("type");
                            String type = typeNode.toString();
                            System.out.println(type);

                            if(!type.equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#List")){
                                String entityQuery = "select distinct ?s where { ?s a <" + type + "> . }";
                                QueryExecution entityExec = QueryExecutionFactory.create(entityQuery,model1);
                                tdb1.begin(ReadWrite.READ);
                                List<QuerySolution> entities = ResultSetFormatter.toList(entityExec.execSelect());
                                entityExec.close();
                                tdb1.end();

                                for(QuerySolution entitySolution : entities){
                                    String entity = entitySolution.get("s").toString();

                                    String query = "construct { <" + entity.toString() + ">  ?p ?o . } where { <" + entity.toString() + "> ?p ?o . }";
                                    QueryExecution qExec1 = QueryExecutionFactory.create(query,model1);
                                    tdb1.begin(ReadWrite.READ);
                                    Model entityModel1 = qExec1.execConstruct();
                                    qExec1.close();
                                    tdb1.end();

                                    QueryExecution qExec2 = QueryExecutionFactory.create(query,model2);
                                    tdb2.begin(ReadWrite.READ);
                                    Model entityModel2 = qExec2.execConstruct();
                                    qExec2.close();
                                    tdb2.end();

                                    List<Statement> list1 = entityModel1.listStatements().toList();
                                    List<Statement> list2 = entityModel2.listStatements().toList();

                                    Map<String, Pair<Double,Integer>> predicateCountOccurenceMap = getInnerMap(overlapIndex,type);
                                    OverlapComputation.compute(list1, list2, predicateCountOccurenceMap);

                                    overlapIndex.put(type,predicateCountOccurenceMap);
                                }
                            }
                        }catch (Exception e){
                            System.out.println("Error");
                        }
                    }

                    OverlapWriter.writeEvaluation(ep1, ep2, overlapIndex);
                }
            }
        }
    }

    private static Map<String, Pair<Double,Integer>> getInnerMap(OverlapIndex overlapIndex, String type)
    {
        Map<String, Pair<Double,Integer>> relationsMap = new HashMap<>();

        if(overlapIndex.containsKey(type)){
            relationsMap = overlapIndex.get(type);
        }

        return relationsMap;
    }

    private static List<String> init(){
        try {
            String content = Files.readString(Path.of("res\\\\management\\\\federations\\\\selection2.json"));
            JSONObject json = new JSONObject(content);

            List<String> endpoints = new LinkedList<>();
            JSONArray array = json.getJSONArray("endpoints");
            for (int i = 0; i < array.length(); i++) {
                endpoints.add(array.get(i).toString());
            }

            return endpoints;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
