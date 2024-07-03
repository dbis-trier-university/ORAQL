package Evaluation;

import Configuration.ConfigurationManagement;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.tdb2.TDB2Factory;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.Scanner;

public class SelectionEvaluation {
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        String dblpPath = "C:\\Databases\\sample_dbs\\dblp_1520\\tdb";

        Dataset dblpDataset = TDB2Factory.connectDataset(dblpPath);
        Model model =dblpDataset.getDefaultModel();
        System.out.println(dblpPath);

        System.out.print("Select Query File: ");
        String queryFilePath = ConfigurationManagement.getSourceFolder() + "/queries/" + sc.nextLine() + ".sparql";
        try {
            String queryStr = Files.readString(Paths.get(queryFilePath));
            Query query = QueryFactory.create(queryStr);
            QueryExecution qExec = QueryExecutionFactory.create(query,model);

            dblpDataset.begin(ReadWrite.READ);
            ResultSet result = qExec.execSelect();
            try {
                ResultSetFormatter.out(new FileOutputStream("res/select_eval.txt"),result,query);
                dblpDataset.end();
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
