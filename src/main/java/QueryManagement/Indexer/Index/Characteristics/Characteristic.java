package QueryManagement.Indexer.Index.Characteristics;

import java.util.*;

public class Characteristic {
    private Set<String> properties;
    private Map<String,Double> types;

    /*******************************************************************************************************************
     * Constructor
     ******************************************************************************************************************/

    public Characteristic() {
        this.properties = new HashSet<>();
        this.types = new HashMap<>();
    }

    /*******************************************************************************************************************
     * Public Methods
     ******************************************************************************************************************/

    public void add(String property){
        this.properties.add(property);
    }

    public void addType(String type){
        if(this.types.containsKey(type)){
            this.types.put(type,this.types.get(type) + 1);
        } else {
            this.types.put(type,1.0);
        }
    }

    public void addType(String type, double count){
        this.types.put(type,count);
    }

    public boolean contains(String property){
        return this.properties.contains(property);
    }

    public boolean containsProperties(Collection<String> properties){
        return this.properties.containsAll(properties);
    }

    public int size(){
        return this.properties.size();
    }

    public Set<String> entrySet(){
        return this.properties;
    }

    public Map<String,Double> getTypesMap() {
        return types;
    }

    public Set<String> getTypes(){
        Set<String> classSet = new HashSet<>();

        for(Map.Entry<String,Double> classEntry : this.types.entrySet()){
            classSet.add(classEntry.getKey());
        }

        return classSet;
    }

    public String toString(){
        StringBuilder str = new StringBuilder("{ ");

        List<String> properties = this.properties.stream().toList();
        for (int i = 0; i < size(); i++) {
            if(i == size()-1){
                str.append(properties.get(i)).append(" }");
            } else {
                str.append(properties.get(i)).append(", ");
            }
        }

        return str.toString();
    }

    @Override
    public int hashCode() {
        return this.properties.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof Characteristic){
            if(((Characteristic) obj).size() == this.size()){

                for(String property : ((Characteristic) obj).entrySet()){
                    if(!this.contains(property)) return false;
                }

                return true;
            } else {
                return false;
            }
        } else {
            return false;
        }
    }
}
