package QueryManagement.DataCrawler;

import QueryManagement.Requests.HttpResponse;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;

import java.io.IOException;

public class Transformer {
    public static Model transformToModel(HttpResponse response){
        String content = response.getContent();
        return transformToModel(content);
    }

    public static Model transformToModel(String content){
        try {
            Model model = ModelFactory.createDefaultModel().read(IOUtils.toInputStream(content, "UTF-8"), null, "N-TRIPLES");

            return model;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
