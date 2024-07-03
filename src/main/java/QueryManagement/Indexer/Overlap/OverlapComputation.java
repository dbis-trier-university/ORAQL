package QueryManagement.Indexer.Overlap;

import org.apache.jena.atlas.lib.Pair;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.vocabulary.RDF;

import java.util.List;
import java.util.Map;

import static org.sotorrent.stringsimilarity.set.Variants.twoGramOverlapNormalized;

public class OverlapComputation {
    public static void compute(List<Statement> ep1Statements, List<Statement> ep2Statements, Map<String, Pair<Double,Integer>> map) {
        for(Statement ep1Statement : ep1Statements){
            Property p1 = ep1Statement.getPredicate();
            boolean added = false;

            for(Statement ep2Statement: ep2Statements){
                Property p2 = ep2Statement.getPredicate();

                if(p1.equals(p2)){
                    RDFNode o1 = ep1Statement.getObject();
                    RDFNode o2 = ep2Statement.getObject();

                    if(!o1.isAnon() && !o2.isAnon()){
                        if(map.containsKey(p1.toString())){
                            map.put(p1.toString(),new Pair<>(map.get(p1.toString()).getLeft() + 1,map.get(p1.toString()).getRight() + 1));
                        } else {
                            map.put(p1.toString(),new Pair<>(1.0,1));
                        }
                        added = true;
                    }

                    break;
                }
            }

            if(!added && !ep1Statement.getObject().isAnon()){
                if(map.containsKey(p1.toString())){
                    map.put(p1.toString(),new Pair<>(map.get(p1.toString()).getLeft(),map.get(p1.toString()).getRight() + 1));
                } else {
                    map.put(p1.toString(),new Pair<>(0.0,1));
                }
            }
        }
    }
}
