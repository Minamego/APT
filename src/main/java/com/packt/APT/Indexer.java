package com.packt.APT;

import com.mongodb.client.FindIterable;
import org.bson.Document;

import java.io.*;
import java.util.*;

public class Indexer {
    private static Map<String, Boolean> isStopWord;
    private static dbConnector db;
    private static Stem stemmer;
    private static Map<String, Content> terms = new HashMap<String, Content>();

    private static void readStopWords() throws IOException {
        isStopWord = new HashMap<String, Boolean>();

        FileInputStream file = new FileInputStream("/home/mina/IdeaProjects/APT/src/main/java/com/packt/APT/stopwords.txt");
        BufferedReader br = new BufferedReader(new InputStreamReader(file));

        String word;
        while ((word = br.readLine()) != null) {
            isStopWord.put(word, true);
            //System.out.println ('"' + word + '"'+',');
        }

        br.close();
    }
    private static void extractWords(String url, ArrayList<String> texts, ArrayList<Integer> tags , int idx) {
        int id = db.getLinkID(url);
        int wordIdx = 0;
        for (int i = 0; i < texts.size(); ++i) {
            String[] words = texts.get(i).split(" ");
            int tag = tags.get(i);
            int len = words.length;
            for (int j = 0; j < len; ++j) {
                String cur = words[j].trim();
                if (isStopWord.containsKey(cur) || cur.length() < 2) continue;
                cur = stemmer.stemWord(cur.toLowerCase());
                Content curContent = terms.get(cur);
                if(curContent == null)
                {
                    curContent = new Content();
                   // System.out.println(cur);
                }
                curContent.add(id , idx , wordIdx++ , tag);
                terms.put(cur , curContent);
            }
        }
    }

    private static void index() {
        terms.clear();

        FindIterable<Document> iterDoc = db.getToBeIndexed();
        int idx = 0;
        for (Document doc : iterDoc) {
            String url = doc.getString("url");
            ArrayList<String> texts = (ArrayList<String>) doc.get("url_data");
            ArrayList<Integer> tags = (ArrayList<Integer>) doc.get("tags");
            extractWords(url , texts , tags , idx);
            idx++;
           // System.out.println(idx);
            System.out.println(idx);
        }
        db.updateToIndex();
        /*************/
        int siz = terms.size();
        for (Map.Entry<String, Content> term : terms.entrySet()) {
            Content c = term.getValue();
            c.addLast();
            db.insertTerm(term.getKey(), c.urls, c.pos, c.tags);
            System.out.println(siz--);
        }

    }

    public static void main(String[] args) throws IOException {
        readStopWords();
        db = new dbConnector();
        db.printTerms();
        stemmer = new Stem();
        while (true)
        {
            try {
                index();
                db.printTerms();
                System.out.println("finished");
                Thread.sleep(1000*60*2);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
    }
}
