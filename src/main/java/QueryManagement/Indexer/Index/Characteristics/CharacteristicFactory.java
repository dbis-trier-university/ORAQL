package QueryManagement.Indexer.Index.Characteristics;

import Configuration.ConfigurationManagement;
import QueryManagement.Indexer.Index.EpIndex;
import QueryManagement.Indexer.TripleFactory;
import QueryManagement.Utils.Endpoint;
import org.apache.jena.rdf.model.*;
import org.apache.jena.vocabulary.RDF;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class CharacteristicFactory {

    /*******************************************************************************************************************
     * Public Methods
     ******************************************************************************************************************/
    public static CharSet create(Endpoint ep, Model model){
        ResIterator subjectsIt = model.listSubjects();
        Map<Characteristic, Info> characteristics = createSet(subjectsIt,model);

        return new CharSet(ep,characteristics);
    }

    public static void write(EpIndex index){
        JSONObject obj = parseToJson(index);

        new File(ConfigurationManagement.getSourceFolder() + "/index/"+index.getEndpoint().getLabel()+"/").mkdirs();
        File file = new File(ConfigurationManagement.getSourceFolder() + "/index/"+index.getEndpoint().getLabel()+"/characteristics.json");

        writeCharacteristics(file,obj);
    }

    public static boolean existsIndex(Endpoint ep){
        return new File(ConfigurationManagement.getSourceFolder() + "/index/"+ep.getLabel()+"/").exists();
    }

    /*******************************************************************************************************************
     * Private Methods
     ******************************************************************************************************************/

    private static Map<Characteristic, Info> createSet(ResIterator subjects, Model model){
        Map<Characteristic, Info> characteristics = new HashMap<>();
        Map<Characteristic,List<List<Statement>>> charTriples = new HashMap<>();

        collectCharacteristicTriples(subjects,model,characteristics,charTriples);
        createMultiplicity(characteristics,charTriples);

        return characteristics;
    }

    private static void collectCharacteristicTriples(ResIterator subjects,
                                                     Model model,
                                                     Map<Characteristic, Info> characteristics,
                                                     Map<Characteristic,List<List<Statement>>> charTriples)
    {
        while (subjects.hasNext()){
            Resource subject = subjects.next();

            // Collect subject triples
            List<Statement> subjectStatements = TripleFactory.querySubjectTriples(subject.toString(), model);

            // Create and add characteristic set
            Characteristic characteristic = createCharacteristic(subjectStatements);
            if(characteristics.containsKey(characteristic)){
                Info info = characteristics.get(characteristic);
                info.increaseCounter();
                characteristics.put(characteristic, info);
            } else {
                characteristics.put(characteristic,new Info());
            }

            // Keep track of triples
            List<List<Statement>> updatedCharTriples;
            if(charTriples.containsKey(characteristic)){
                updatedCharTriples = charTriples.get(characteristic);
            } else {
                updatedCharTriples = new LinkedList<>();
            }
            updatedCharTriples.add(subjectStatements);
            charTriples.put(characteristic,updatedCharTriples);
        }
    }

    private static void createMultiplicity(Map<Characteristic, Info> characteristics,
                                           Map<Characteristic,List<List<Statement>>> charTriples)
    {
        for(Map.Entry<Characteristic,List<List<Statement>>> entry : charTriples.entrySet()){
            Characteristic characteristic = entry.getKey();

            Map<String,Double> multiplicityMap = new HashMap<>();
            for(String property : characteristic.entrySet()){
                double counter = 0;

                for(List<Statement> record : entry.getValue()){
                    counter += countPropertyUsage(property,record);
                }

                counter = counter/entry.getValue().size();
                multiplicityMap.put(property,counter);
            }

            for(List<Statement> record : entry.getValue()){
                for(Statement stmt : record){
                    if(stmt.getPredicate().toString().equals(RDF.type.toString())){
                        characteristic.addType(stmt.getObject().toString());
                    }
                }
            }

            characteristics.get(characteristic).setMultiplicityMap(multiplicityMap);
        }
    }

    private static int countPropertyUsage(String property, List<Statement> record){
        int counter = 0;

        for(Statement stmt : record){
            if(stmt.getPredicate().toString().equals(property))
                counter++;
        }

        return counter;
    }

    private static int countTypeUsage(String type, List<Statement> record){
        int counter = 0;

        for(Statement stmt : record){
            if(stmt.getPredicate().toString().equals(RDF.type.toString()) && stmt.getObject().toString().equals(type))
                counter++;
        }

        return counter;
    }

    private static Characteristic createCharacteristic(List<Statement> subjectStatements){
        Characteristic characteristic = new Characteristic();

        for(Statement stmt : subjectStatements){
            if(stmt.getPredicate().equals(RDF.type)) characteristic.addType(stmt.getObject().toString());
            characteristic.add(stmt.getPredicate().toString());
        }

        return characteristic;
    }

    private static JSONObject parseToJson(EpIndex index){
        JSONObject obj = new JSONObject();
        obj.put("endpoint",index.getEndpoint().getLabel());
        obj.put("triples",index.getTriples());

        JSONArray sets = createSets(index.getCharSet(),index.getTriples());
        obj.put("sets",sets);

        JSONObject types = createTypeSet(index.getCharSet());
        obj.put("types",types);

        return obj;
    }

    private static JSONArray createSets(CharSet charSet, long triples){
        JSONArray sets = new JSONArray();

        // Sort according to counter
        List<Map.Entry<Characteristic, Info>> list = new ArrayList<>(charSet.entrySet().stream().toList());
        list.sort(Comparator.comparingLong(o -> o.getValue().getCounter()));
        Collections.reverse(list);

        long counter = 0;
        for(Map.Entry<Characteristic, Info> characteristic : list){
            JSONObject setInfo = new JSONObject();

            JSONArray properties = new JSONArray();
            for(String property : characteristic.getKey().entrySet()){
                properties.put(property);
            }

            JSONObject types = new JSONObject();
            for(Map.Entry<String,Double> entry : characteristic.getKey().getTypesMap().entrySet()){
                types.put(entry.getKey(),entry.getValue());
            }

            setInfo.put("counter",characteristic.getValue().getCounter());
            setInfo.put("multiplicity",characteristic.getValue().getMultiplicityMap());
            setInfo.put("coverage",characteristic.getValue().getCoverage());
            setInfo.put("estimation",(characteristic.getValue().getCoverage() * triples));
            setInfo.put("properties",properties);
            setInfo.put("types",types);
            sets.put(setInfo);

            counter += (long) (characteristic.getValue().getCoverage() * triples);
        }

        System.out.println(triples + " -> " + counter);

        return sets;
    }

    private static JSONObject createTypeSet(CharSet charSet){

        Map<String,Double> typesMap = new HashMap<>();

        List<Map.Entry<Characteristic, Info>> list = new ArrayList<>(charSet.entrySet().stream().toList());
        for(Map.Entry<Characteristic, Info> characteristic : list){
            for(Map.Entry<String,Double> entry : characteristic.getKey().getTypesMap().entrySet()){
                if(typesMap.containsKey(entry.getKey())){
                    typesMap.put(entry.getKey(),typesMap.get(entry.getKey()) + entry.getValue());
                } else {
                    typesMap.put(entry.getKey(),entry.getValue());
                }
            }
        }

        JSONObject types = new JSONObject();
        for(Map.Entry<String,Double> entry : typesMap.entrySet()){
            types.put(entry.getKey(),entry.getValue());
        }

        return types;
    }

    private static void writeCharacteristics(File file, JSONObject indexObj){
        try {
            FileWriter writer = new FileWriter(file);
            writer.write(indexObj.toString());
            writer.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
