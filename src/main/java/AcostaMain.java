import Configuration.ConfigurationManagement;
import Configuration.EndpointManagement;
import Configuration.UserPreferences;
import QueryManagement.Indexer.Baseline.AcostaProcessor;
import QueryManagement.Utils.Endpoint;
import QueryManagement.Utils.Federation;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.Model;

import java.util.Map;
import java.util.Scanner;

public class AcostaMain {
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        ConfigurationManagement.init();
        EndpointManagement management = EndpointManagement.getInstance();

        System.out.print("Select Query File: ");
        String queryFilePath = ConfigurationManagement.getSourceFolder() + "/queries/reliability/" + sc.nextLine() + ".sparql";

        Federation federation = management.loadFederation("reliability2");

        // For now, we will ignore runtime
        double minReliability = 0.7;
        AcostaProcessor qp = new AcostaProcessor(queryFilePath,federation,minReliability);
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
