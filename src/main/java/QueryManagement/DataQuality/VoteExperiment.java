package QueryManagement.DataQuality;

import QueryManagement.Utils.Endpoint;
import org.apache.jena.atlas.lib.Pair;
import org.apache.jena.rdf.model.Statement;
import org.opencompare.hac.experiment.Experiment;

import java.util.List;

public class VoteExperiment implements Experiment {
    private List<Pair<Endpoint,Statement>> elements;

    public VoteExperiment(List<Pair<Endpoint,Statement>> elements) {
        this.elements = elements;
    }

    @Override
    public int getNumberOfObservations() {
        return elements.size();
    }

    public String getObject(int i){
        return elements.get(i).getRight().getObject().toString();
    }
    public String getProperty(int i){
        return elements.get(i).getRight().getPredicate().getLocalName();
    }
}
