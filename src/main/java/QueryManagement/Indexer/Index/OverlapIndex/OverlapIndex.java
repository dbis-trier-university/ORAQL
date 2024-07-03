package QueryManagement.Indexer.Index.OverlapIndex;

import org.apache.jena.atlas.lib.Pair;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class OverlapIndex {
    private Map<String, Map<String, Pair<Double,Integer>>> typeMap;

    /*******************************************************************************************************************
     * Constructor
     ******************************************************************************************************************/

    public OverlapIndex() {
        this.typeMap = new HashMap<>();
    }

    /*******************************************************************************************************************
     * Public Methods
     ******************************************************************************************************************/

    public Map<String, Pair<Double,Integer>> get(String type){
        return this.typeMap.get(type);
    }

    public int getPredicateOccurrence(String type, String predicate){
        if(type == null){
            int size = 0;

            for(Map.Entry<String,Map<String,Pair<Double,Integer>>> typeEntry : this.typeMap.entrySet()){
                for(Map.Entry<String,Pair<Double,Integer>> predicateEntry : typeEntry.getValue().entrySet()){
                    if(predicateEntry.getKey().equals(predicate)){
                        size += predicateEntry.getValue().getRight();
                    }
                }
            }

            return size;
        } else {
            return this.typeMap.get(type).get(predicate).getRight();
        }
    }

    public double getPredicateOverlap(String type, String predicate){
        if(type == null){
            double equal = 0;

            for(Map.Entry<String,Map<String,Pair<Double,Integer>>> typeEntry : this.typeMap.entrySet()){
                for(Map.Entry<String,Pair<Double,Integer>> predicateEntry : typeEntry.getValue().entrySet()){
                    if(predicateEntry.getKey().equals(predicate)){
                        equal += predicateEntry.getValue().getLeft();
                    }
                }
            }

            return equal/getPredicateOccurrence(type,predicate);
        } else {
            Pair<Double,Integer> pair = this.typeMap.get(type).get(predicate);
            return pair.getLeft()/pair.getRight();
        }
    }

    public void put(String type, Map<String, Pair<Double,Integer>> relationsMap){
        this.typeMap.put(type,relationsMap);
    }

    public boolean containsKey(String type){
        return this.typeMap.containsKey(type);
    }

    public Set<Map.Entry<String, Map<String, Pair<Double,Integer>>>> entrySet(){
        return this.typeMap.entrySet();
    }
}
