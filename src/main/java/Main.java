import Configuration.ConfigurationManagement;
import Configuration.EndpointManagement;
import Configuration.UserPreferences;
import QueryManagement.Processor.QueryProcessor;
import QueryManagement.Utils.Endpoint;
import QueryManagement.Utils.Federation;

import java.util.Scanner;

public class Main {
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        ConfigurationManagement.init();

        System.out.print("Select Query File: ");
        String queryFilePath = ConfigurationManagement.getSourceFolder() + "/queries/eval/" + sc.nextLine() + ".sparql";

        System.out.print("Select Federation: ");
        EndpointManagement management = EndpointManagement.getInstance();
        Federation federation = management.loadFederation(sc.nextLine());

        System.out.print("Minimum Reliability: ");
        double minReliability = Double.parseDouble(sc.nextLine());
        UserPreferences preferences = new UserPreferences(minReliability,Integer.MAX_VALUE);

        QueryProcessor qp = new QueryProcessor(queryFilePath,federation,preferences);
        qp.query();

        System.out.println("DONE");
    }
}