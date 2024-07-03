package QueryManagement.DataQuality;

import org.opencompare.hac.experiment.DissimilarityMeasure;
import org.opencompare.hac.experiment.Experiment;

public class TwoGramOverlapDissimilarity implements DissimilarityMeasure {
    @Override
    public double computeDissimilarity(Experiment experiment, int i, int j) {
        VoteExperiment gapExperiment = (VoteExperiment) experiment;
        return (1 - org.sotorrent.stringsimilarity.set.Variants.twoGramOverlapNormalized(gapExperiment.getObject(i), gapExperiment.getObject(j)));
    }
}
