package QueryManagement.DataCrawler;

import QueryManagement.Utils.Endpoint;
import QueryManagement.Voting.Voting;
import org.apache.jena.atlas.lib.Pair;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.*;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class ModelCreator {

    /*******************************************************************************************************************
     * Public Method
     ******************************************************************************************************************/

    public static Model createMergedModel(Map<Endpoint, Model> tripleResults, Triple triple){
        Model mergedModel = ModelFactory.createDefaultModel();

        Model tmpModel = ModelCreator.merge(tripleResults,triple);
        mergedModel.add(tmpModel);

        return mergedModel;
    }

    public static Model createMergedModel(Map<Endpoint, Model> tripleResults){
        Model mergedModel = ModelFactory.createDefaultModel();

        for(Map.Entry<Endpoint,Model> entry : tripleResults.entrySet()){
            mergedModel.add(entry.getValue());
        }

        return mergedModel;
    }

    /*******************************************************************************************************************
     * Public Method
     ******************************************************************************************************************/

    private static Model merge(Map<Endpoint,Model> endpointResults, Triple triple){
        List<Map.Entry<Endpoint,Model>> list = endpointResults.entrySet().stream().toList();

        Model finalModel = ModelFactory.createDefaultModel();

        Model model = list.get(0).getValue();
        ResIterator it = model.listSubjects();
        while (it.hasNext()){
            Resource entity = it.next();
            List<Statement> stmtList = getSubjectStatements(model,entity);

            List<Pair<Endpoint,List<Statement>>> statements = new LinkedList<>();
            statements.add(new Pair<>(list.get(0).getKey(),stmtList));

            for (int i = 1; i < list.size(); i++) {
                Model tmpModel = list.get(i).getValue();
                List<Statement> tmpStmtList = getSubjectStatements(tmpModel,entity);
                statements.add(new Pair<>(list.get(i).getKey(),tmpStmtList));
            }

            List<Pair<Endpoint,List<Statement>>> filteredStatements = filter(statements,triple.getPredicate().toString());
            List<Statement> votedStatement = Voting.vote(filteredStatements);
            if(!votedStatement.isEmpty()) finalModel.add(votedStatement);
        }

        return finalModel;
    }

    private static List<Statement> getSubjectStatements(Model model, Resource entity){
        StmtIterator it = model.listStatements();

        List<Statement> list = new LinkedList<>();
        while (it.hasNext()){
            Statement stmt = it.next();

            if(stmt.getSubject().equals(entity)) list.add(stmt);
        }

        return list;
    }

    private static List<Pair<Endpoint,List<Statement>>> filter(List<Pair<Endpoint,List<Statement>>> statements, String property){
        List<Pair<Endpoint,List<Statement>>> filteredStatements = new LinkedList<>();

        for (Pair<Endpoint,List<Statement>> statement : statements) {
            List<Statement> list = statement.getRight().stream()
                    .filter(stmt -> stmt.getPredicate().toString().equals(property)).toList();
            filteredStatements.add(new Pair<>(statement.getLeft(),list));
        }

        return filteredStatements;
    }

}
