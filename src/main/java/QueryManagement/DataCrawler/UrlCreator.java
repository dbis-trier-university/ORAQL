package QueryManagement.DataCrawler;

import org.apache.jena.graph.Triple;

import java.net.URLEncoder;

public class UrlCreator {
    public static String prepareUrl(String basicUrl, Triple triple){
        String url = basicUrl;

        if(triple.getSubject().isVariable()){
            url = url.replace("{s}","");
        } else {
            url = url.replace("{s}", URLEncoder.encode(triple.getSubject().toString()));
        }

        if(triple.getPredicate().isVariable()){
            url = url.replace("{p}","");
        } else {
            url = url.replace("{p}",URLEncoder.encode(triple.getPredicate().toString()));
        }

        if(triple.getObject().isVariable()){
            url = url.replace("{o}","");
        } else if(triple.getObject().isLiteral()){
            url = url.replace("{o}",URLEncoder.encode(triple.getObject().getLiteral().toString()));
        } else {
            url = url.replace("{o}",URLEncoder.encode(triple.getObject().toString()));
        }

        return url;
    }

    public static String prepareSubjectUrl(String basicUrl,Triple triple, String subject){
        String url = basicUrl;
        url = url.replace("{s}", URLEncoder.encode(subject));

        if(triple.getPredicate().isVariable()){
            url = url.replace("{p}","");
        } else {
            url = url.replace("{p}",URLEncoder.encode(triple.getPredicate().toString()));
        }

        if(triple.getObject().isVariable()){
            url = url.replace("{o}","");
        } else if(triple.getObject().isLiteral()){
            url = url.replace("{o}",URLEncoder.encode(triple.getObject().getLiteral().toString()));
        } else {
            url = url.replace("{o}",URLEncoder.encode(triple.getObject().toString()));
        }

        return url;
    }

    public static String prepareObjectUrl(String basicUrl,Triple triple, String object){
        String url = basicUrl;

        if(triple.getSubject().isVariable()){
            url = url.replace("{s}","");
        } else {
            url = url.replace("{s}",URLEncoder.encode(triple.getSubject().toString()));
        }

        if(triple.getPredicate().isVariable()){
            url = url.replace("{p}","");
        } else {
            url = url.replace("{p}",URLEncoder.encode(triple.getPredicate().toString()));
        }

        url = url.replace("{o}",URLEncoder.encode(object));

        return url;
    }
}
