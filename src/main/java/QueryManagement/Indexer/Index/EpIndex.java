package QueryManagement.Indexer.Index;

import QueryManagement.Indexer.Index.Characteristics.Info;
import QueryManagement.Indexer.Index.Characteristics.CharSet;
import QueryManagement.Indexer.Index.Characteristics.Characteristic;
import QueryManagement.Indexer.Index.OverlapIndex.OverlapIndex;
import QueryManagement.Utils.Endpoint;
import org.apache.jena.rdf.model.Model;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class EpIndex {
    private Endpoint endpoint;
    private long triples;
    private long sampleSize;
    private CharSet charSet;
    private Map<Endpoint,OverlapIndex> overlapIndexMap;
    private Map<String,Set<String>> entitiesTypeMap;
    private Model sampleModel;

    /*******************************************************************************************************************
     * Constructor
     ******************************************************************************************************************/

    public EpIndex(Endpoint endpoint) {
        this.endpoint = endpoint;
        this.triples = -1;
        this.charSet = new CharSet(this.endpoint);
        this.entitiesTypeMap = new HashMap<>();
        this.overlapIndexMap = new HashMap<>();
    }

    /*******************************************************************************************************************
     * Public Methods
     ******************************************************************************************************************/

    public Endpoint getEndpoint() {
        return endpoint;
    }

    public long getTriples() {
        return triples;
    }

    public CharSet getCharSet() {
        return charSet;
    }

    public Map<String,Set<String>> getEntitiesTypeMap() {
        return entitiesTypeMap;
    }

    public Model getSampleModel() {
        return sampleModel;
    }

    public OverlapIndex getEpIndex(Endpoint ep){
        return this.overlapIndexMap.get(ep);
    }

    public void setEndpoint(Endpoint endpoint) {
        this.endpoint = endpoint;
    }

    public void setTriples(long triples) {
        if(this.triples <= 0) this.triples = triples;
    }

    public void setSampleSize(long sampleSize) {
        this.sampleSize = sampleSize;
    }

    public void setEntitiesTypeMap(Map<String, Set<String>> entitiesTypeMap) {
        this.entitiesTypeMap = entitiesTypeMap;
    }

    public void setCharSet(CharSet charSet) {
        this.charSet = charSet;
    }

    public void addOverlapIndex(Endpoint ep, OverlapIndex overlapIndex){
        this.overlapIndexMap.put(ep,overlapIndex);
    }

    public void setSampleModel(Model sampleModel) {
        this.sampleModel = sampleModel;
    }

    public void extrapolate(){
        for(Map.Entry<Characteristic, Info> entry : this.charSet.entrySet()){
            double coverage = 0;

            for(String property : entry.getKey().entrySet()){
                coverage += (entry.getValue().getMultiplicityMap().get(property) * entry.getValue().getCounter());
            }

            coverage = coverage /this.sampleSize;
            entry.getValue().setCoverage(coverage);
        }
    }
}
