package QueryManagement.Processor;

import Configuration.UserPreferences;
import QueryManagement.DataCrawler.Crawler;
import QueryManagement.DataCrawler.Probing;
import QueryManagement.Indexer.Indexer;
import QueryManagement.SourceSelection.SourceSelection;
import QueryManagement.TripleSegmentation.TripleVisitor;
import QueryManagement.Utils.Federation;
import QueryManagement.Utils.ProbingResult;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpWalker;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

public class QueryProcessor {
    private Query query;
    private QueryPlan queryPlan;
    private List<ProbingResult> probingResults;

    /*******************************************************************************************************************
     * Constructor
     ******************************************************************************************************************/

    public QueryProcessor(String queryFilePath, Federation federation, UserPreferences preferences) {
        // Create indexer for overlap information
        Indexer indexer = new Indexer(federation);

        // Create query and extract triples
        this.query = createQueryObject(queryFilePath);
        List<Triple> triples = extractTriples();

        // Start probing and source selection
        this.probingResults = Probing.start(indexer,triples,federation);

        //Start with source selection
        this.queryPlan = SourceSelection.startSourceSelection(indexer,probingResults,preferences);
    }

    /*******************************************************************************************************************
     * Public Methods
     ******************************************************************************************************************/

    public void query(){
        // Execute generated query plan and create a result model
        Instant start = Instant.now();
        Model model = Crawler.executeQueryPlan(queryPlan,probingResults);
        Instant finish = Instant.now();
        long timeElapsed = Duration.between(start, finish).toSeconds();
        System.out.println("Crawl and Vote Time: " + timeElapsed);

        queryModel(model,this.query);
    }

    public QueryPlan getQueryPlan() {
        return queryPlan;
    }

    public List<ProbingResult> getProbingResults() {
        return probingResults;
    }

    /*******************************************************************************************************************
     * Private Methods
     ******************************************************************************************************************/

    private Query createQueryObject(String queryFilePath){
        String queryStr;

        try {
            queryStr = Files.readString(Paths.get(queryFilePath));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return QueryFactory.create(queryStr);
    }

    private List<Triple> extractTriples(){
        Op op = Algebra.compile(query);
        TripleVisitor visitor = new TripleVisitor();
        OpWalker.walk(op,visitor);

        return visitor.getTriples();
    }

    private void queryModel(Model model, Query query){
        QueryExecution qExec = QueryExecutionFactory.create(query,model);

        ResultSet result = qExec.execSelect();
        try {
            ResultSetFormatter.out(new FileOutputStream("res/result.txt"),result,query);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

}
