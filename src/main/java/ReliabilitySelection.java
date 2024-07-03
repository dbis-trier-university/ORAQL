import Configuration.ConfigurationManagement;
import Configuration.EndpointManagement;
import Configuration.UserPreferences;
import QueryManagement.Processor.QueryProcessor;
import QueryManagement.Utils.Endpoint;
import QueryManagement.Utils.Federation;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.Model;

import java.util.Map;
import java.util.Scanner;

public class ReliabilitySelection {
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        ConfigurationManagement.init();

        System.out.print("Select Query File: ");
        String queryFilePath = ConfigurationManagement.getSourceFolder() + "/queries/reliability/" + sc.nextLine() + ".sparql";

        EndpointManagement management = EndpointManagement.getInstance();
        Federation federation = management.loadFederation("reliability2");

        double minReliability = 0.7;
        UserPreferences preferences = new UserPreferences(minReliability,Integer.MAX_VALUE);
        QueryProcessor qp = new QueryProcessor(queryFilePath,federation,preferences);

        System.out.println("Source Selection:");
        for(Map.Entry<Triple, Map<Endpoint, Model>> triple : qp.getQueryPlan().entrySet()){
            System.out.println("Triple: " + triple.getKey());

            for(Map.Entry<Endpoint,Model> endpoint : triple.getValue().entrySet()){
                System.out.println(endpoint.getKey().getLabel());
            }

            System.out.println();
        }
        qp.query();

        System.out.println("DONE");
    }
}