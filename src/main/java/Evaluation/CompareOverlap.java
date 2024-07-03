package Evaluation;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class CompareOverlap {
    public static void main(String[] args) {
        List<String> files = init();

        for(String file1 : files){
            for(String file2 : files){
                if(!file1.equals(file2)){
                    String gsPath = "res\\index\\goldstandard2\\" + file1 + "\\" + file1 + "_" + file2 + ".json";
                    String path = "res\\index\\" + file1 + "\\" + file2 + ".json";

                    try{
                        String output = "";

                        String gsContent = Files.readString(Path.of(gsPath));
                        JSONObject gsJson = new JSONObject(gsContent);

                        String content = Files.readString(Path.of(path));
                        JSONObject json = new JSONObject(content);

                        Iterator<String> gsIt = gsJson.keys();
                        while (gsIt.hasNext()){
                            String gsClass = gsIt.next();

                            JSONObject gsRelObject = gsJson.getJSONObject(gsClass);
                            Iterator<String> gsRelIt = gsRelObject.keys();
                            while (gsRelIt.hasNext()){
                                String gsRel = gsRelIt.next();

                                double gsOverlap = gsRelObject.getJSONObject(gsRel).getDouble("overlap");
                                try{
                                    JSONObject classJson = json.getJSONObject(gsClass);

                                    try{
                                        double overlap = classJson.getJSONObject(gsRel).getDouble("overlap");
                                        output += gsClass + ", " + gsRel + ", " + (overlap - gsOverlap) + "\n";
                                    } catch (JSONException innerE){
                                        output += gsClass + ", " + gsRel + ", RELATION NOT FOUND\n";
                                    }

                                } catch (JSONException outerE){
                                    output += gsClass + ", " + gsRel + ", CLASS NOT FOUND\n";
//                                    break;
                                }

                            }

                        }

                        File tmp = new File("res\\index\\comparison\\");
                        tmp.mkdirs();

                        File file = new File("res\\index\\comparison\\" + file1 + "_" + file2 + ".csv");
                        BufferedWriter writer = new BufferedWriter(new FileWriter(file));
                        writer.write(output);
                        writer.close();

                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
    }

    private static List<String> init(){
        try {
            String content = Files.readString(Path.of("res\\\\management\\\\federations\\\\selection2.json"));
            JSONObject json = new JSONObject(content);

            List<String> endpoints = new LinkedList<>();
            JSONArray array = json.getJSONArray("endpoints");
            for (int i = 0; i < array.length(); i++) {
                endpoints.add(array.get(i).toString());
            }

            return endpoints;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
