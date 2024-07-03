package Evaluation;

import Configuration.ConfigurationManagement;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Scanner;

import static org.apache.jena.tdb2.TDB2Factory.connectDataset;

public class QueryEvaluation {
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);

        System.out.print("Select Query File: ");
        String queryFilePath = ConfigurationManagement.getSourceFolder() + "/queries/reliability/" + sc.nextLine() + ".sparql";

        String query;
        try {
            query = Files.readString(Paths.get(queryFilePath));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // Creation of a construction query object
        Query queryObject = QueryFactory.create(query);
        Dataset dataset = connectDataset("C:\\Databases\\sample_dbs\\dblp_1520_752\\tdb");
        Model model = dataset.getDefaultModel();
        QueryExecution exe = QueryExecutionFactory.create(queryObject,model);

        // Querying dataset
        dataset.begin(ReadWrite.READ);
        ResultSet resultSet = exe.execSelect();
        try {
            ResultSetFormatter.out(new FileOutputStream("res/result.txt"),resultSet);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }

        exe.close();
        dataset.end();
        System.out.println("Done.");
    }
}
