package Configuration;

import QueryManagement.Utils.Endpoint;
import QueryManagement.Utils.Federation;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;

public class EndpointManagement {
    private static final EndpointManagement endpointManagement = new EndpointManagement();
    private List<Endpoint> endpointList;

    /*******************************************************************************************************************
     * Constructors
     ******************************************************************************************************************/

    private EndpointManagement() {
        this.endpointList = loadEndpoints();
    }

    /*******************************************************************************************************************
     * Public Methods
     ******************************************************************************************************************/

    public static EndpointManagement getInstance(){
        return endpointManagement;
    }

    public Endpoint getEndpoint(String label){
        for(Endpoint ep : this.endpointList){
            if(ep.getLabel().equalsIgnoreCase(label)) return ep;
        }

        return null;
    }

    public List<Endpoint> loadEndpoints(){

        try {
            String path = ConfigurationManagement.getSourceFolder() + "management/endpoints.json";
            String content = Files.readString(Path.of(path));
            JSONObject obj = new JSONObject(content);

            JSONArray epArray = obj.getJSONArray("endpoints");
            if(!epArray.isEmpty()){
                List<Endpoint> endpointsList = new LinkedList<>();
                for (int i = 0; i < epArray.length(); i++) {
                    JSONObject epObj = epArray.getJSONObject(i);
                    Endpoint ep = parseToEndpoint(epObj);
                    endpointsList.add(ep);
                }

                return endpointsList;
            }

            return null;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Federation loadFederation(String label){
        Federation federation = new Federation(label);

        String path = ConfigurationManagement.getSourceFolder() + "management/federations/" + label + ".json";
        String content = null;
        try {
            content = Files.readString(Path.of(path));
            JSONArray members = new JSONObject(content).getJSONArray("endpoints");
            for (int i = 0; i < members.length(); i++) federation.addEndpoint(getEndpoint(members.getString(i)));

            return federation;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /*******************************************************************************************************************
     * Private Methods
     ******************************************************************************************************************/

    private static Endpoint parseToEndpoint(JSONObject epObj){
        String label = epObj.getString("label");
        String name = epObj.getString("name");
        String url = epObj.getString("url");
        try{
            double reliability = epObj.getDouble("reliability");
            int time = epObj.getInt("time");

            return new Endpoint(label,name,url,reliability,time);
        }catch (Exception e){
            return new Endpoint(label,name,url);
        }
    }
}
