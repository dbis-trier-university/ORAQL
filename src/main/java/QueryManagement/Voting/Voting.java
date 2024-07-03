package QueryManagement.Voting;

import QueryManagement.DataQuality.TwoGramOverlapDissimilarity;
import QueryManagement.DataQuality.VoteExperiment;
import QueryManagement.Utils.Endpoint;
import org.apache.jena.atlas.lib.Pair;
import org.apache.jena.rdf.model.Statement;
import org.opencompare.hac.HierarchicalAgglomerativeClusterer;
import org.opencompare.hac.agglomeration.AgglomerationMethod;
import org.opencompare.hac.agglomeration.AverageLinkage;
import org.opencompare.hac.dendrogram.*;
import org.opencompare.hac.experiment.DissimilarityMeasure;
import org.opencompare.hac.experiment.Experiment;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;

public class Voting {
    public static List<Statement> vote(List<Pair<Endpoint,List<Statement>>> filteredStatements){
        List<Pair<Endpoint,Statement>> result = new LinkedList<>();

        boolean functional = true;
        for(Pair<Endpoint,List<Statement>> stmts : filteredStatements){
            if(stmts.getRight().size() == 1){
                result.add(new Pair<>(stmts.getLeft(),stmts.getRight().get(0)));
            } else if(stmts.getRight().size() > 1) {
                functional = false;
                for(Statement stmt : stmts.getRight()){
                    result.add(new Pair<>(stmts.getLeft(),stmt));
                }
            }
        }

        List<Statement> statements = new LinkedList<>();
        if(functional){
            if(result.size() == 1){
                statements.add(result.get(0).getRight());
            } else if(result.size() == 2){
                if(result.get(0).getLeft().getReliability() > result.get(1).getLeft().getReliability()){
                    statements.add(result.get(0).getRight());
                } else if(result.get(0).getLeft().getReliability() < result.get(1).getLeft().getReliability()){
                    statements.add(result.get(1).getRight());
                } else {
                    Random rnd = new Random();
                    int selection = rnd.nextInt(2);
                    statements.add(result.get(selection).getRight());
                }
            } else if(result.size() > 2) {
                Dendrogram dendrogram = computeDendrogram(result);
                DendrogramNode node = cutOffAt(0.7,(MergeNode) dendrogram.getRoot());
                double confidence = node.getObservationCount()/((double) result.size());

                if(confidence >= 0.5) statements.add(determineIntegrationValue(result, node));
                else System.out.println("Not confident enough: " + confidence);
            }

            return statements;
        } else {
            for(Pair<Endpoint,Statement> pair : result){
                statements.add(pair.getRight());
            }

            return statements;
        }
    }

    private static Dendrogram computeDendrogram(List<Pair<Endpoint,Statement>> result){
        Experiment experiment = new VoteExperiment(result);
        DissimilarityMeasure dissimilarityMeasure = new TwoGramOverlapDissimilarity();
        AgglomerationMethod agglomerationMethod = new AverageLinkage();
        DendrogramBuilder dendrogramBuilder = new DendrogramBuilder(experiment.getNumberOfObservations());
        HierarchicalAgglomerativeClusterer clusterer = new HierarchicalAgglomerativeClusterer(experiment, dissimilarityMeasure, agglomerationMethod);
        clusterer.cluster(dendrogramBuilder);

        return dendrogramBuilder.getDendrogram();
    }

    private static DendrogramNode cutOffAt(double threshold, MergeNode node){
        while (node.getDissimilarity() > threshold){
            if(node.getLeft() instanceof MergeNode mergeLeft && node.getRight() instanceof MergeNode mergeRight){
                if(mergeLeft.getDissimilarity() >= mergeRight.getDissimilarity()){
                    node = mergeRight;
                } else {
                    node = mergeLeft;
                }
            } if(node.getLeft() instanceof MergeNode mergeNode){
                node = mergeNode;
            } else if(node.getRight() instanceof MergeNode mergeNode) {
                node = mergeNode;
            } else {
                return node.getLeft();
            }
        }

        return node;
    }

    private static Statement determineIntegrationValue(List<Pair<Endpoint,Statement>> result, DendrogramNode node){
        DendrogramNode dNode = node;

        while (dNode.getLeft() != null || dNode.getRight() != null){
            if(dNode.getLeft() instanceof MergeNode mergeLeft && dNode.getRight() instanceof MergeNode mergeRight){
                if(mergeLeft.getDissimilarity() >= mergeRight.getDissimilarity()){
                    dNode = dNode.getRight();
                } else {
                    dNode = dNode.getLeft();
                }
            } if(dNode.getLeft() instanceof MergeNode mergeNode){
                dNode = dNode.getLeft();
            } else if(dNode.getRight() instanceof MergeNode mergeNode) {
                dNode = dNode.getRight();
            } else {
                dNode = dNode.getLeft();
            }
        }

        return result.get(((ObservationNode) dNode).getObservation()).getRight();
    }
}
