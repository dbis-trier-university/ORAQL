import Configuration.ConfigurationManagement;
import Configuration.EndpointManagement;
import Configuration.UserPreferences;
import QueryManagement.Processor.QueryProcessor;
import QueryManagement.Utils.Endpoint;
import QueryManagement.Utils.Federation;
import QueryManagement.Utils.ProbingResult;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.Model;

import java.util.Map;
import java.util.Scanner;

public class EndpointSelection {
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        ConfigurationManagement.init();

        System.out.print("Select Query File: ");
        String queryFilePath = ConfigurationManagement.getSourceFolder() + "/queries/eval/" + sc.nextLine() + ".sparql";

        EndpointManagement management = EndpointManagement.getInstance();
        Federation federation = management.loadFederation("selection2");

        // For now, we will ignore runtime
        double minReliability = 0.5;
        UserPreferences preferences = new UserPreferences(minReliability,Integer.MAX_VALUE);
        QueryProcessor qp = new QueryProcessor(queryFilePath,federation,preferences);

        System.out.println("Probing Results:");
        for(ProbingResult result : qp.getProbingResults()){
            System.out.println(result.getTriple()+ ": " + result.getAllEndpointResults().size() + " -> " + result.getAllEndpointResults());
        }

        System.out.println("\nSource Selection:");
        for(Map.Entry<Triple, Map<Endpoint, Model>> triple : qp.getQueryPlan().entrySet()){
            System.out.println("Triple: " + triple.getKey());

            for(Map.Entry<Endpoint,Model> endpoint : triple.getValue().entrySet()){
                System.out.println(endpoint.getKey().getLabel());
            }

            System.out.println();
        }
    }
}