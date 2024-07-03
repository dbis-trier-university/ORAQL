package QueryManagement.Indexer.Loader;

import Configuration.ConfigurationManagement;
import Configuration.EndpointManagement;
import QueryManagement.Indexer.Index.EpIndex;
import QueryManagement.Indexer.Index.OverlapIndex.OverlapIndex;
import QueryManagement.Utils.Endpoint;
import org.apache.jena.atlas.lib.Pair;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class OverlapLoader {

    /*******************************************************************************************************************
     * Public Methods
     ******************************************************************************************************************/

    public static void load(List<EpIndex> indexList){
        for(EpIndex index : indexList) loadOverlapIndex(index);
    }

    /*******************************************************************************************************************
     * Private Methods
     ******************************************************************************************************************/

    private static void loadOverlapIndex(EpIndex index){
        String indexPath = ConfigurationManagement.getSourceFolder() + "index/" + index.getEndpoint().getLabel() + "/";

        Set<String> overlapFilePaths = listOverlapIndexes(indexPath);
        for(String overlapFilePath : overlapFilePaths){
            OverlapIndex overlapIndex = parseJson(indexPath,overlapFilePath);
            updateIndex(overlapFilePath,index,overlapIndex);
        }
    }

    private static OverlapIndex parseJson(String indexPath, String overlapFilePath){
        try {
            OverlapIndex overlapIndex = new OverlapIndex();
            String overlapString = Files.readString(Path.of(indexPath + overlapFilePath));

            JSONObject overlapObj = new JSONObject(overlapString);

            Iterator<String> typeIterator = overlapObj.keys();
            while (typeIterator.hasNext()){
                String typeKey = typeIterator.next();

                Map<String, Pair<Double,Integer>> relationsMap = new HashMap<>();
                JSONObject relationObjectList = overlapObj.getJSONObject(typeKey);
                Iterator<String> relationIterator = relationObjectList.keys();
                while (relationIterator.hasNext()){
                    String relationKey = relationIterator.next();

                    JSONObject relationObject = relationObjectList.getJSONObject(relationKey);
                    double equal = relationObject.getDouble("equal");
                    int size = relationObject.getInt("size");

                    relationsMap.put(relationKey,new Pair<>(equal,size));
                }

                overlapIndex.put(typeKey,relationsMap);
            }

            return overlapIndex;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void updateIndex(String overlapFilePath, EpIndex index, OverlapIndex overlapIndex){
        String epLabel = overlapFilePath.replace(".json","");
        Endpoint ep = EndpointManagement.getInstance().getEndpoint(epLabel);
        index.addOverlapIndex(ep,overlapIndex);
    }

    private static Set<String> listOverlapIndexes(String dir) {
        Set<String> set = Stream.of(new File(dir).listFiles())
                .filter(file -> !file.isDirectory())
                .map(File::getName)
                .collect(Collectors.toSet());
        set.remove("characteristics.json");

        return set;
    }
}
