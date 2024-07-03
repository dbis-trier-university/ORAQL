package QueryManagement.Indexer.Index.Characteristics;

import QueryManagement.Utils.Endpoint;

import java.util.*;

public class CharSet {
    private Endpoint endpoint;
    private Map<Characteristic, Info> characteristics;

    /*******************************************************************************************************************
     * Constructor
     ******************************************************************************************************************/

    public CharSet(Endpoint endpoint) {
        this.endpoint = endpoint;
        this.characteristics = new HashMap<>();
    }

    /*******************************************************************************************************************
     * Public Methods
     ******************************************************************************************************************/

    public CharSet(Endpoint endpoint, Map<Characteristic, Info> characteristics) {
        this.endpoint = endpoint;
        this.characteristics = characteristics;
    }

    public Endpoint getEndpoint() {
        return endpoint;
    }

    public Set<String> getClasses(){
        Set<String> classSet = new HashSet<>();

        for(Map.Entry<Characteristic,Info> entry : this.characteristics.entrySet()){
            classSet.addAll(entry.getKey().getTypes());
        }

        return classSet;
    }

    public boolean containsAll(Collection<String> properties){
        for(Map.Entry<Characteristic,Info> entry : this.characteristics.entrySet()){
            if(entry.getKey().containsProperties(properties)) return true;
        }

        return false;
    }

    public boolean contains(String property){
        for(Map.Entry<Characteristic,Info> entry : this.characteristics.entrySet()){
            if(entry.getKey().contains(property)) return true;
        }

        return false;
    }

    public Set<Map.Entry<Characteristic, Info>> entrySet(){
        return this.characteristics.entrySet();
    }

}
