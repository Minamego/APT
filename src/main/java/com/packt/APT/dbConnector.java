package com.packt.APT;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.bson.Document;

import java.util.*;

public class dbConnector {
    private MongoClient mongoClient;
    private MongoDatabase database;
    MongoCollection<Document> terms;

    private boolean collectionExists(final String collectionName) {
        MongoIterable<String> collectionNames = database.listCollectionNames();
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
        if (!collectionExists("words")) {
            database.createCollection("words");
        }
        terms = database.getCollection("words");

    }

    public void clean() {
        terms.deleteMany(new Document());
    }

    public void insertTerm(String term , Set<String> urls, ArrayList<ArrayList<Integer>> pos , ArrayList<ArrayList<Integer>> tags) {

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
        for(String url : urls)
        {
            // loop on each url
            page = new Document("positions", pos.get(i)).append("tag", tags.get(i));
            // update url
            updateQueryAppendToArray = new BasicDBObject();
            updateQueryAppendToArray.put("$addToSet", new BasicDBObject().append("urls", url));
            updateQueryAppendToArray.put("$set", new BasicDBObject().append("details." + url, page));

            // apply the update to the database
            BasicDBObject updateQuery = new BasicDBObject("term", term);
            terms.updateOne(updateQuery, updateQueryAppendToArray);
            i++;
        }

    }

    public void printDocs() {
        // Getting the iterable object
        FindIterable<Document> iterDoc = terms.find();
        // Getting the iterator
        Iterator it = iterDoc.iterator();

        while (it.hasNext()) {
            System.out.println(it.next());
        }
    }

    public void close() {
        mongoClient.close();
    }
}
