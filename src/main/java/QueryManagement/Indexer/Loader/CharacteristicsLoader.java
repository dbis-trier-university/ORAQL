package QueryManagement.Indexer.Loader;

import Configuration.ConfigurationManagement;
import QueryManagement.Indexer.Index.Characteristics.Info;
import QueryManagement.Indexer.Index.Characteristics.CharSet;
import QueryManagement.Indexer.Index.Characteristics.Characteristic;
import QueryManagement.Indexer.Index.EpIndex;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class CharacteristicsLoader {

    /*******************************************************************************************************************
     * Public Methods
     ******************************************************************************************************************/
    public static void load(EpIndex index){
        String indexPath = ConfigurationManagement.getSourceFolder() + "index/" + index.getEndpoint().getLabel();

        try {
            // Read File
            String charString = Files.readString(Path.of(indexPath + "/characteristics.json"));
            JSONObject charObj = new JSONObject(charString);

            // Create CharSet
            Map<Characteristic, Info> characteristics = loadCharacteristicsMap(charObj);
            CharSet charSet = new CharSet(index.getEndpoint(),characteristics);

            // Update Index
            index.setTriples(charObj.getInt("triples"));
            index.setCharSet(charSet);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /*******************************************************************************************************************
     * Private Methods
     ******************************************************************************************************************/

    private static Map<Characteristic, Info> loadCharacteristicsMap(JSONObject charObj){
        Map<Characteristic, Info> characteristics = new HashMap<>();

        JSONArray array = charObj.getJSONArray("sets");
        for (int i = 0; i < array.length(); i++) {
            Characteristic characteristic = new Characteristic();
            JSONObject innerObj = array.getJSONObject(i);

            addTypes(innerObj,characteristic);
            addProperties(innerObj,characteristic);
            Map<String,Double> multiplicityMap = createMultiplicityMap(innerObj);

            Info info = new Info();
            info.setCoverage(innerObj.getDouble("coverage"));
            info.setMultiplicityMap(multiplicityMap);

            characteristics.put(characteristic,info);
        }

        return characteristics;
    }

    private static void addTypes(JSONObject innerObj, Characteristic characteristic){
        JSONObject typeObj = innerObj.getJSONObject("types");
        Iterator<String> typeIterator = typeObj.keys();

        while (typeIterator.hasNext()) {
            String type = typeIterator.next();
            characteristic.addType(type,typeObj.getInt(type));
        }
    }

    private static void addProperties(JSONObject innerObj, Characteristic characteristic){
        JSONArray properties = innerObj.getJSONArray("properties");
        for (int j = 0; j < properties.length(); j++) {
            characteristic.add(properties.getString(j));
        }
    }

    private static Map<String,Double> createMultiplicityMap(JSONObject innerObj){
        Map<String,Double> multiplicityMap = new HashMap<>();

        JSONObject multiplicity = innerObj.getJSONObject("multiplicity");
        Iterator<String> it = multiplicity.keys();
        while (it.hasNext()){
            String property = it.next();
            multiplicityMap.put(property,multiplicity.getDouble(property));
        }

        return multiplicityMap;
    }
}
