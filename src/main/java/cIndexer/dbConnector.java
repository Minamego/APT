package cIndexer;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import org.bson.Document;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.*;

public class dbConnector {
    private MongoClient mongoClient;
    private MongoDatabase database;
    MongoCollection<Document> terms;
    MongoCollection<Document> documents;
    MongoCollection<Document> links;
    private  Map<String , Integer> linksID;
    private  int maxLinkID;
    List<WriteModel<Document>> termsUpdates = new ArrayList<WriteModel<Document>>();
    List<WriteModel<Document>> linksUpdates = new ArrayList<WriteModel<Document>>();

    private boolean collectionExists( String collectionName , MongoIterable<String> collectionNames) {

        for (final String name : collectionNames) {
            if (name.equalsIgnoreCase(collectionName)) {
                return true;
            }
        }
        return false;
    }

    dbConnector() {
        mongoClient = new MongoClient("localhost" , 27017);
        database = mongoClient.getDatabase("APT");
        MongoIterable<String> collectionNames = database.listCollectionNames();

        if (!collectionExists("words" , collectionNames)) {
            database.createCollection("words");
        }
        if (!collectionExists("documents" , collectionNames)) {
            database.createCollection("documents");
        }
        if (!collectionExists("links" , collectionNames)) {
            database.createCollection("links");
        }
        terms = database.getCollection("words");
        documents = database.getCollection("documents");
        links = database.getCollection("links");
        // clean();
        readLinks();
    }

    // clean database
    public void clean() {
        terms.deleteMany(new Document());
        links.deleteMany(new Document());
    }
    // add all data of a specific term to the database
    public void addTermUpdate(String term , ArrayList<Integer> goodUrls , ArrayList<Integer> totUrls , ArrayList<ArrayList<Integer>> pos , ArrayList<ArrayList<Integer>> tags)
    {
        Document page = new Document();
        BasicDBObject updateQuery;
        BasicDBObject updateObject = new BasicDBObject("term", term);
        termsUpdates.add(
                new UpdateOneModel<Document>(
                        updateObject, // filter
                        new Document("$set", new Document("term", term)),  // update
                        new UpdateOptions().upsert(true)  // options like upsert
                )
        );
        for(int url : totUrls)
        {
            updateQuery = new BasicDBObject();
            updateQuery.put("$unset", new BasicDBObject().append("details." + url, page));
            termsUpdates.add(
                    new UpdateOneModel<Document>(
                            updateObject, // filter
                            updateQuery  // update
                    )
            );
        }
        for(int i = 0 ; i<goodUrls.size() ; ++i)
        {
            int url = goodUrls.get(i);
            page = new Document("positions", pos.get(i)).append("tag", tags.get(i));
            updateQuery = new BasicDBObject();
            updateQuery.put("$set", new BasicDBObject().append("details." + url, page));
            termsUpdates.add(
                    new UpdateOneModel<Document>(
                            updateObject, // filter
                            updateQuery  // update
                    )
            );
        }
        applyUpdates();
    }

    // apply all the current updates to the databae
    public  void  applyUpdates()
    {
        if(!termsUpdates.isEmpty()) terms.bulkWrite(termsUpdates);
        if(!linksUpdates.isEmpty()) links.bulkWrite(linksUpdates);
        termsUpdates.clear();
        linksUpdates.clear();
    }

    // read links id from database
    public void readLinks()
    {
        linksID = new HashMap<String, Integer>();
        FindIterable<Document> iterDoc = links.find();
        maxLinkID = 0;
        for(Document doc : iterDoc)
        {
            int id =  doc.getInteger("id");
            linksID.put(doc.getString("url") ,id);
            maxLinkID = Math.max(id , maxLinkID);
        }
    }

    // add link to database
    public void addLink(String url)
    {
        maxLinkID++;
        Document page = new Document("url", url)
                .append("id" , maxLinkID);
        linksUpdates.add(
                new InsertOneModel<Document>(
                        page
                )
        );
        linksID.put(url , maxLinkID);
    }
    public int getLinkID(String url)
    {
        if(!linksID.containsKey(url)) addLink(url);
        return linksID.get(url);
    }

    public void printTerms() throws FileNotFoundException {
        PrintWriter writer = new PrintWriter("out.txt");
        // Getting the iterable object
        FindIterable<Document> iterDoc = terms.find();
        // Getting the iterator
        Iterator it = iterDoc.iterator();

        while (it.hasNext()) {
            writer.println(it.next());
        }
        iterDoc = links.find();
        // Getting the iterator
        it = iterDoc.iterator();

        while (it.hasNext()) {
            writer.println(it.next());
        }
        writer.close();

    }


    // get all documents that should be indexed from database
    public  FindIterable<Document> getToBeIndexed()
    {
        FindIterable<Document> iterDoc  = documents.find(new BasicDBObject("to_index", 1));
        return  iterDoc;
    }

    // reset the indexed documents index flag to zero
    public void updateToIndex()
    {
        FindIterable<Document> iterDoc  = documents.find(new BasicDBObject("to_index", 1));

        BasicDBObject updateQuery = new BasicDBObject();
        updateQuery.put("$set", new BasicDBObject().append("to_index", 0));
        List<WriteModel<Document>> updates = new ArrayList<WriteModel<Document>>();
        // apply the update to the database
        for(Document doc : iterDoc)
        {
            BasicDBObject updateObject = new BasicDBObject("url", doc.get("url"));
            updates.add(
                    new UpdateOneModel<Document>(
                            updateObject, // filter
                            updateQuery  // update
                    )
            );
        }
        if(!updates.isEmpty()) documents.bulkWrite(updates);

    }
}