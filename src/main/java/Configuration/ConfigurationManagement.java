package Configuration;

import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class ConfigurationManagement {
    private static final Path configPath = Path.of("config.json");

    public static void init(){
        // Create query folder
        boolean queries = new File(getSourceFolder()+"/queries").mkdirs();

        // Create federation folder
        boolean management = new File(getSourceFolder()+"/management/federations").mkdirs();

        // Create logs folder
        boolean logs = new File(getSourceFolder()+"/logs").mkdirs();

        if(queries) System.out.println("Created folder for queries..");
        if(management) System.out.println("Created federation management folders ..");
        if(logs) System.out.println("Created log folder");
    }

    public static String getSourceFolder(){
        try {
            String content = Files.readString(configPath);
            JSONObject obj = new JSONObject(content);

            return obj.getString("source");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static int getTrainingSize(){
        try {
            String content = Files.readString(configPath);
            JSONObject obj = new JSONObject(content);

            return obj.getInt("training");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static int getEntitiesSize(){
        try {
            String content = Files.readString(configPath);
            JSONObject obj = new JSONObject(content);

            return obj.getInt("entities");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static int getMinIndexOccurrence(){
        try {
            String content = Files.readString(configPath);
            JSONObject obj = new JSONObject(content);

            return obj.getInt("min_occurrence");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static boolean additionalCrawling(){
        try {
            String content = Files.readString(configPath);
            JSONObject obj = new JSONObject(content);

            return obj.getBoolean("additional_crawling");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
