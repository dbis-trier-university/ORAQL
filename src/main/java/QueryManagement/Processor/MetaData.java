package QueryManagement.Processor;

import org.apache.jena.rdf.model.*;

public class MetaData {
    public static Model getMetaModel(Model model){
        StmtIterator it = model.listStatements();

        Resource metaSubject = null;
        while (it.hasNext()){
            Statement stmt = it.nextStatement();

            if(stmt.getPredicate().toString().equalsIgnoreCase("http://rdfs.org/ns/void#triples")){
                metaSubject = stmt.getSubject();
                break;
            }
        }

        Model metaModel = ModelFactory.createDefaultModel();
        it = model.listStatements();
        while (metaSubject != null && it.hasNext()){
            Statement stmt = it.nextStatement();

            if(stmt.getSubject().equals(metaSubject)){
                metaModel.add(stmt);
            }
        }

        return metaModel;
    }

    public static Model removeMetaData(Model model){
        Model metaModel = getMetaModel(model);
        model.remove(metaModel);

        return metaModel;
    }

    public static String nextPageUrl(Model metaModel){
        StmtIterator it = metaModel.listStatements();

        while (it.hasNext()){
            Statement stmt = it.nextStatement();

            if(stmt.getPredicate().toString().equalsIgnoreCase("http://www.w3.org/ns/hydra/core#nextPage")){
                return stmt.getObject().toString();
            }
        }

        return null;
    }

    public static boolean hasNextPage(Model metaModel){
        StmtIterator it = metaModel.listStatements();

        while (it.hasNext()){
            Statement stmt = it.nextStatement();

            if(stmt.getPredicate().toString().equalsIgnoreCase("http://www.w3.org/ns/hydra/core#nextPage")){
                return true;
            }
        }

        return false;
    }

    public static long getResultSize(Model metaModel){
        StmtIterator it = metaModel.listStatements();

        while (it.hasNext()){
            Statement stmt = it.nextStatement();

            if(stmt.getPredicate().toString().equalsIgnoreCase("http://rdfs.org/ns/void#triples")){
                return Long.parseLong(stmt.getObject().toString());
            }
        }

        return -1;
    }

    public static long getNumberOfPages(Model model, Model metaModel){
        long triples = getResultSize(metaModel);
        double pageSize = (model.listStatements().toList().size() - metaModel.listStatements().toList().size());

        return (long) Math.ceil((triples / pageSize));
    }
}
