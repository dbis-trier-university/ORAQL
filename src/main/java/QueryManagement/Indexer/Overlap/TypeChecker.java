package QueryManagement.Indexer.Overlap;

import org.apache.jena.vocabulary.OWL;
import org.apache.jena.vocabulary.RDF;

public class TypeChecker {
    public static boolean isGenericType(String relation){
        return relation.equals(RDF.List.toString()) || relation.equals(OWL.Thing.toString());
    }
}
