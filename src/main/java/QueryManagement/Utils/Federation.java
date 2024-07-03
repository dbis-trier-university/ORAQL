package QueryManagement.Utils;

import org.apache.jena.dboe.migrate.L;

import java.util.LinkedList;
import java.util.List;

public class Federation {
    private String label;
    private List<Endpoint> members;

    /*******************************************************************************************************************
     * Constructors
     ******************************************************************************************************************/

    public Federation(String label) {
        this.label = label;
        this.members = new LinkedList<>();
    }

    public Federation(String label, List<Endpoint> members) {
        this.label = label;
        this.members = members;
    }

    /*******************************************************************************************************************
     * Public Methods
     ******************************************************************************************************************/

    public void addEndpoint(Endpoint ep){
        this.members.add(ep);
    }

    public String getLabel() {
        return label;
    }

    public List<Endpoint> getMembers() {
        return members;
    }

    public Endpoint getMember(String label){
        for(Endpoint ep : this.members){
            if(ep.getLabel().equalsIgnoreCase(label)) return ep;
        }

        return null;
    }
}