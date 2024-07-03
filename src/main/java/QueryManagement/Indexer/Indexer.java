package QueryManagement.Indexer;

import Configuration.ConfigurationManagement;
import Configuration.EndpointManagement;
import QueryManagement.DataCrawler.Transformer;
import QueryManagement.Indexer.Index.Characteristics.CharSet;
import QueryManagement.Indexer.Index.Characteristics.CharacteristicFactory;
import QueryManagement.Indexer.Index.EpIndex;
import QueryManagement.Indexer.Loader.CharacteristicsLoader;
import QueryManagement.Indexer.Loader.OverlapLoader;
import QueryManagement.Indexer.Overlap.OverlapFactory;
import QueryManagement.Processor.MetaData;
import QueryManagement.Requests.HttpHandler;
import QueryManagement.Requests.HttpResponse;
import QueryManagement.Utils.Endpoint;
import QueryManagement.Utils.Federation;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.vocabulary.RDF;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

public class Indexer {
    private List<EpIndex> indexList = new LinkedList<>();

    /*******************************************************************************************************************
     * Constructor
     ******************************************************************************************************************/

    public Indexer(Federation federation) {
        System.out.println("Start Indexing");
        Instant start = Instant.now();

        boolean existing = initCharacteristics(federation);
        initOverlap(existing);

        Instant finish = Instant.now();
        long timeElapsed = Duration.between(start, finish).toSeconds();
        System.out.println("Index Time: " + timeElapsed);
    }

    /*******************************************************************************************************************
     * Public Methods
     ******************************************************************************************************************/

    public EpIndex getEpIndex(String label){
        for(EpIndex index : this.indexList){
            if(index.getEndpoint().getLabel().equalsIgnoreCase(label)) return index;
        }

        return null;
    }

    /*******************************************************************************************************************
     * Private Methods
     ******************************************************************************************************************/

    private boolean initCharacteristics(Federation federation){
        List<Endpoint> endpointList = federation.getMembers();

        boolean existing = false;
        for(Endpoint ep : endpointList){
            EpIndex index = new EpIndex(ep);

            if(CharacteristicFactory.existsIndex(ep)){
                CharacteristicsLoader.load(index);
                existing = true;
            } else {
                createIndex(index);
            }

            this.indexList.add(index);
        }

        return existing;
    }

    private void initOverlap(boolean existing){
        if(!existing){
            OverlapFactory overlapFactory = new OverlapFactory(this.indexList);
            overlapFactory.createOverlapIndexes();
        }

        OverlapLoader.load(this.indexList);
    }

    private void createIndex(EpIndex index){
        System.out.println("Create: " + index.getEndpoint().getLabel());

        long triples = ConfigurationManagement.getTrainingSize();
        Model sampleModel = crawlTriples(index,triples);
        CharSet charSet = CharacteristicFactory.create(index.getEndpoint(),sampleModel);

        index.setSampleSize(sampleModel.size());
        index.setCharSet(charSet);
        index.extrapolate();
        index.setEntitiesTypeMap(collectEntities(sampleModel));
        index.setSampleModel(sampleModel);

        CharacteristicFactory.write(index);
    }

    private Model crawlTriples(EpIndex index, long triples){
        String cleanUrl = cleanUrl(index.getEndpoint().getUrl());
        long pages = init(index,cleanUrl);

        Model model = ModelFactory.createDefaultModel();
        Set<Long> called = new HashSet<>();
        while (model.size() < triples && called.size() < pages) {
            Random rndGenerator = new Random();
            long rndPage = rndGenerator.nextLong(pages) + 1; // Random from 0 to pages but pages start at 1
            while (called.contains(rndPage)) {
                // System.out.println(called.size() + " / " + pages + " (" + model.size() + ")");
                rndPage = rndGenerator.nextLong(pages) + 1;
            }

            String currentUrl = cleanUrl + "&page=" + rndPage;
            HttpResponse response = HttpHandler.sendGetRequest(currentUrl,index.getEndpoint().getTime(),0);

            if(response != null && response.getContent() != null){
                String content = TripleFactory.removeFirstAndLast(response);
                Model itModel = Transformer.transformToModel(content);
                MetaData.removeMetaData(itModel);
                model.add(itModel);
            }

            called.add(rndPage);
        }

        return model;
    }

    private String cleanUrl(String url){
        url = url.replace("{s}","");
        url = url.replace("{p}","");
        url = url.replace("{o}","");

        return url;
    }

    private Map<String,Set<String>> collectEntities(Model sampleModel){
        Map<String,Set<String>> entityTypesMap = new HashMap<>();

        StmtIterator it = sampleModel.listStatements();
        while (it.hasNext()){
            Statement res = it.next();

            Set<String> entities;
            if(!res.getSubject().isAnon() && res.getPredicate().equals(RDF.type)) {
                if(entityTypesMap.containsKey(res.getObject().toString()))
                    entities = entityTypesMap.get(res.getObject().toString());
                else
                    entities = new HashSet<>();

                entities.add(res.getSubject().toString());
                entityTypesMap.put(res.getObject().toString(),entities);
            }
        }

        return entityTypesMap;
    }

    private long init(EpIndex index, String url){
        HttpResponse response = HttpHandler.sendGetRequest(url,index.getEndpoint().getTime(),0);

        Model model;
        Model metaModel;
        if(response != null && response.getContent() != null){
            model = Transformer.transformToModel(response);
            metaModel = MetaData.removeMetaData(model);

            long resultSize = MetaData.getResultSize(metaModel);
            index.setTriples(resultSize);

            return (long) Math.ceil(((double) resultSize)/model.size());
        }

        return -1;
    }
}
