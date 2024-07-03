package QueryManagement.Indexer;

import QueryManagement.Requests.HttpResponse;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;

import java.util.LinkedList;
import java.util.List;

public class TripleFactory {
    public static List<Statement> collectSubjectTriples(String subject, Model model){
        List<Statement> subjectStatements = new LinkedList<>();

        StmtIterator it = model.listStatements();
        while (it.hasNext()){
            Statement stmt = it.nextStatement();
            if(stmt.getSubject().toString().equals(subject)) subjectStatements.add(stmt);
        }

        return subjectStatements;
    }

    public static List<Statement> querySubjectTriples(String subject, Model model){
        String queryStr = "construct { <" + subject + "> ?p ?o . } where { <" + subject + "> ?p ?o . }";
        QueryExecution qExec = QueryExecutionFactory.create(queryStr,model);
        Model tmp = qExec.execConstruct();

        return tmp.listStatements().toList();
    }

    public static String removeFirstAndLast(HttpResponse response){
        String[] triples = response.getContent().split("\n");
        String firstEntity = triples[0].split(" ")[0];
        String lastEntity = triples[triples.length-1].split(" ")[0];

        StringBuilder content = new StringBuilder();
        for(String triple : triples){
            if(!triple.startsWith(firstEntity) && !triple.startsWith(lastEntity))
            {
                content.append(triple).append("\n");
            }
        }

        return content.toString();
    }

}
