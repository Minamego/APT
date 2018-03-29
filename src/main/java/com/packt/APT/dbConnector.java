package com.packt.APT;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoWriteException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;

import org.bson.Document;

import javax.print.Doc;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.*;

public class dbConnector {
    private MongoClient mongoClient;
    private MongoDatabase database;
    MongoCollection<Document> terms;
    MongoCollection<Document> documents;
    MongoCollection<Document> links;
    private  Map<String , Integer> linksID;
    private  int maxLinkID;
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
        clean();
        readLinks();
    }

    public void clean() {
        terms.deleteMany(new Document());
        links.deleteMany(new Document());
    }

    public void insertTerm(String term , Set<Integer> urls, ArrayList<ArrayList<Integer>> pos , ArrayList<ArrayList<Integer>> tags) {

        Document page = new Document();
        BasicDBObject updateQueryAppendToArray;

        Document cur = terms.find(new Document("term", term)).first();
        if(cur == null)
        {
            page = new Document("term", term);
            terms.insertOne(page);
        }
        else {
            List<String> docs = (List<String>) cur.get("urls");

            for (String doc : docs) {
                if(!urls.contains(doc))
                {

                    updateQueryAppendToArray = new BasicDBObject();
                    updateQueryAppendToArray.put("$pull", new BasicDBObject().append("urls", doc));
                    updateQueryAppendToArray.put("$unset", new BasicDBObject().append("details." + doc, page));

                    // apply the update to the database
                    BasicDBObject updateQuery = new BasicDBObject("term", term);
                    terms.updateOne(updateQuery, updateQueryAppendToArray);
                }
            }
        }
        int i = 0;
        for(int url : urls)
        {
            // loop on each url
            page = new Document("positions", pos.get(i)).append("tag", tags.get(i));
            // update url
            updateQueryAppendToArray = new BasicDBObject();
            updateQueryAppendToArray.put("$addToSet", new BasicDBObject().append("urls", url));
            updateQueryAppendToArray.put("$set", new BasicDBObject().append("details." + url, page));

            // apply the update to the database
            BasicDBObject updateQuery = new BasicDBObject("term", term);
            try {
                terms.updateOne(updateQuery, updateQueryAppendToArray);
            }
            catch (MongoWriteException e)
            {
                System.out.println(url);
            }
            i++;
        }

    }

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
    public void addLink(String url)
    {
        maxLinkID++;
        Document page = new Document("url", url)
                .append("id" , maxLinkID);
        links.insertOne(page);
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



    public  FindIterable<Document> getToBeIndexed()
    {
        FindIterable<Document> iterDoc  = documents.find(new BasicDBObject("to_index", 1));
        return  iterDoc;
    }
    public void updateToIndex()
    {
        FindIterable<Document> iterDoc  = documents.find(new BasicDBObject("to_index", 1));

        BasicDBObject updateQuery = new BasicDBObject();
        updateQuery.put("$set", new BasicDBObject().append("to_index", 0));

        // apply the update to the database
        for(Document doc : iterDoc)
        {
            BasicDBObject updateObject = new BasicDBObject("url", doc.get("url"));
            documents.updateOne(updateObject, updateQuery);
        }

    }

    public void close() {
        mongoClient.close();
    }
}
