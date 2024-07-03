package QueryManagement.Indexer.Baseline;

import QueryManagement.DataCrawler.Crawler;
import QueryManagement.DataCrawler.Probing;
import QueryManagement.Indexer.Indexer;
import QueryManagement.Processor.QueryPlan;
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

public class AcostaProcessor {
    private static final int BUDGET = 100;
    private Query query;

    private QueryPlan queryPlan;

    private List<ProbingResult> probingResults;

    /*******************************************************************************************************************
     * Constructor
     ******************************************************************************************************************/

    public AcostaProcessor(String queryFilePath, Federation federation, double minReliability) {
        // Create indexer for overlap information
        Indexer indexer = new Indexer(federation);

        // Create query and extract triples
        createQueryObject(queryFilePath);
        List<Triple> triples = extractTriples();

        // Start probing and source selection
        this.probingResults = Probing.start(indexer,triples,federation);

        //Start with source selection
        AcostaSelection as = new AcostaSelection(indexer,BUDGET,minReliability);
        this.queryPlan = as.startSourceSelection(probingResults);
    }

    /*******************************************************************************************************************
     * Public Methods
     ******************************************************************************************************************/

    public void query(){
        // Execute generated query plan and create a result model
        Instant start = Instant.now();
        Model model = Crawler.executeAcostaPlan(queryPlan,probingResults);
        Instant finish = Instant.now();
        long timeElapsed = Duration.between(start, finish).toSeconds();
        System.out.println("Crawl and Vote Time: " + timeElapsed);

        queryModel(model,this.query);
    }

    public QueryPlan getQueryPlan() {
        return queryPlan;
    }

    /*******************************************************************************************************************
     * Private Methods
     ******************************************************************************************************************/

    private void createQueryObject(String queryFilePath){
        String queryStr;

        try {
            queryStr = Files.readString(Paths.get(queryFilePath));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        this.query = QueryFactory.create(queryStr);
    }

    private List<Triple> extractTriples(){
        Op op = Algebra.compile(query);
        TripleVisitor visitor = new TripleVisitor();
        OpWalker.walk(op,visitor);

        return visitor.getTriples();
    }

    private void queryModel(Model model, Query query){
        Instant start = Instant.now();
        QueryExecution qExec = QueryExecutionFactory.create(query,model);

        ResultSet result = qExec.execSelect();
        try {
            ResultSetFormatter.out(new FileOutputStream("res/result.txt"),result,query);

            Instant finish = Instant.now();
            long timeElapsed = Duration.between(start, finish).toSeconds();
            System.out.println("Query Time: " + timeElapsed);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
