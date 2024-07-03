package QueryManagement.Indexer.Overlap;

import QueryManagement.Indexer.Index.OverlapIndex.OverlapInfo;
import QueryManagement.Indexer.Indexer;
import QueryManagement.Utils.Endpoint;
import QueryManagement.Utils.ProbingResult;
import org.apache.jena.atlas.lib.Pair;

import java.util.*;

public class OverlapStack {
    private List<OverlapInfo> overlapList;
    private List<Endpoint> removed;

    public OverlapStack(ProbingResult result, Indexer indexer, Map<String,String> entityClassMap) {
        this.overlapList = new LinkedList<>();
        this.removed = new LinkedList<>();

        for(Pair<Endpoint,Integer> e1 : result.getAllEndpointResults()){
            for(Pair<Endpoint,Integer> e2 : result.getAllEndpointResults()){
                if(!e1.getLeft().getLabel().equals(e2.getLeft().getLabel())){
                    String type = entityClassMap.get(result.getTriple().getSubject().toString());
                    double overlap;
                    try{
                        overlap = indexer.getEpIndex(e1.getLeft().getLabel()).getEpIndex(e2.getLeft()).getPredicateOverlap(type,result.getTriple().getPredicate().toString());
                    }catch (Exception e){
                        overlap = 0.0;
                    }

                    this.overlapList.add(new OverlapInfo(e1,e2,overlap));
                }
            }
        }

        this.overlapList.sort(Comparator.comparingDouble(OverlapInfo::getOverlap));
        Collections.reverse(this.overlapList);
    }

    public double getOverlap(String ep1, String ep2){
        for(OverlapInfo info : this.overlapList){
            if(info.getEp1().getLeft().getLabel().equals(ep1) && info.getEp2().getLeft().getLabel().equals(ep2)){
                return info.getOverlap();
            }
        }

        return -1;
    }

    public boolean isCovered(Endpoint ep){
        List<OverlapInfo> tmp = new LinkedList<>();
        double coverage = 0;

        for(OverlapInfo info : this.overlapList){
            if(info.getEp1().getLeft().getLabel().equals(ep.getLabel()) && info.getOverlap() == 1) {
                if(!removed.contains(info.getEp2().getLeft())){
                    this.removed.add(ep);
                    return true;
                }
            } else if(info.getEp1().getLeft().getLabel().equals(ep.getLabel()) && !removed.contains(info.getEp2().getLeft())) {
                tmp.add(info);
                coverage += info.getOverlap();
            }
        }

        if(coverage >= 1){
            boolean covered = true;
            for(OverlapInfo info1 : tmp){
                for(OverlapInfo info2 : tmp){
                    if(getOverlap(info1.getEp2().getLeft().getLabel(),info2.getEp2().getLeft().getLabel()) > 0){
                        covered = false;
                        break;
                    }
                }
            }

            if(covered) this.removed.add(ep);
            return covered;
        }

        return false;
    }
}
