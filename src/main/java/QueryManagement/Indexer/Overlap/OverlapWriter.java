package QueryManagement.Indexer.Overlap;

import Configuration.ConfigurationManagement;
import QueryManagement.Indexer.Index.OverlapIndex.OverlapIndex;
import QueryManagement.Utils.Endpoint;
import org.apache.jena.atlas.lib.Pair;
import org.json.JSONObject;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

public class OverlapWriter {
    public static void write(Endpoint ep1, Endpoint ep2, OverlapIndex index){
        JSONObject obj = createJson(index);

        writeFile(ep1.getLabel(),ep2.getLabel(),obj);
//        writeFile(ep2.getLabel(),ep1.getLabel(),obj);
    }


    private static JSONObject createJson(OverlapIndex index){
        JSONObject obj = new JSONObject();

        for(Map.Entry<String,Map<String,Pair<Double,Integer>>> typeEntry : index.entrySet()){
            JSONObject typeObject = new JSONObject();

            for(Map.Entry<String,Pair<Double,Integer>> entry : typeEntry.getValue().entrySet()){
                JSONObject meta = new JSONObject();
                meta.put("overlap",entry.getValue().getLeft()/entry.getValue().getRight());
                meta.put("equal",entry.getValue().getLeft());
                meta.put("size",entry.getValue().getRight());

                typeObject.put(entry.getKey(),meta);
            }

            obj.put(typeEntry.getKey(),typeObject);
        }

        return obj;
    }

    private static void writeFile(String ep1Label, String ep2Label, JSONObject obj){
        String src = ConfigurationManagement.getSourceFolder();

        File file = new File(src + "/index/"+ep1Label+"/"+ep2Label+".json");
        try {
            FileWriter writer = new FileWriter(file);
            writer.write(obj.toString());
            writer.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void writeEvaluation(String epLabel1, String epLabel2, OverlapIndex index){
        JSONObject obj = createJson(index);

        writeEval(epLabel1,epLabel2,obj);
    }

    private static void writeEval(String ep1Label, String ep2Label, JSONObject obj){
        String src = ConfigurationManagement.getSourceFolder();

        File file = new File(src + "/index/"+ep1Label+"_"+ep2Label+".json");
        try {
            FileWriter writer = new FileWriter(file);
            writer.write(obj.toString());
            writer.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
