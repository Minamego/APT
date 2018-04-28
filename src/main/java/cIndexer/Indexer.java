package cIndexer;

import com.mongodb.client.FindIterable;
import org.bson.Document;

import java.io.*;
import java.util.*;

public class Indexer {
    private static Map<String, Boolean> isStopWord;
    private static dbConnector db;
    private static Stem stemmer;
    private static Map<String, Content> terms = new HashMap<String, Content>();

    // read stop stop words from file
    private static void readStopWords() throws IOException {
        isStopWord = new HashMap<String, Boolean>();

        FileInputStream file = new FileInputStream("/home/mina/IdeaProjects/Indexer/src/main/java/cIndexer/stopwords.txt");
        BufferedReader br = new BufferedReader(new InputStreamReader(file));

        String word;
        while ((word = br.readLine()) != null) {
            isStopWord.put(word, true);
        }

        br.close();
    }

    // extract all words from a url and save all of their data, positions , tags
    private static void extractWords(String url, ArrayList<String> texts, ArrayList<Integer> tags , int idx) {
        int id = db.getLinkID(url);
        int wordIdx = 0;
        for (int i = 0; i < texts.size(); ++i) {
            String[] words = texts.get(i).split(" ");
            int tag = tags.get(i);
            int len = words.length;
            for (int j = 0; j < len; ++j) {
                wordIdx++;
                String cur = words[j].trim().toLowerCase();
                if (isStopWord.containsKey(cur) || cur.length() < 2) continue;
                Content curContent = terms.get(cur);
                if(curContent == null)
                {
                    curContent = new Content();
                }
                curContent.add(id , idx , wordIdx , tag);
                terms.put(cur , curContent);
                String afterStem = stemmer.stemWord(cur);  // stem the word to its origin
                if(afterStem == cur) continue;
                cur = afterStem;
                curContent = terms.get(cur);
                if(curContent == null)
                {
                    curContent = new Content();
                }
                curContent.add(id , idx , wordIdx , tag);
                terms.put(cur , curContent);
            }
        }
    }

    // read all the documents that should be indexed and extract data from them then add them in special format to the database
    private static void index() {
        terms.clear();

        FindIterable<Document> iterDoc = db.getToBeIndexed();
        ArrayList<Integer> totUrls = new ArrayList<Integer>();
        int idx = 0;
        for (Document doc : iterDoc) {
            String url = doc.getString("url");
            ArrayList<String> texts = (ArrayList<String>) doc.get("url_data");
            ArrayList<Integer> tags = (ArrayList<Integer>) doc.get("tags");
            extractWords(url , texts , tags , idx);
            totUrls.add(db.getLinkID(url));
            idx++;
            System.out.println(idx);
        }
        db.updateToIndex();
        /*************/
        int siz = terms.size();
        for (Map.Entry<String, Content> term : terms.entrySet()) {
            Content c = term.getValue();
            c.addLast();
            System.out.println("start");
            db.addTermUpdate(term.getKey(), c.urls ,totUrls , c.pos, c.tags);
            System.out.println("finish");
            System.out.println(siz--);
        }

    }

    public static void main(String[] args) throws IOException {
        readStopWords();
        db = new dbConnector();
        stemmer = new Stem();
        while (true)
        {
            try {
                index();
                System.out.println("finished");
                Thread.sleep(1000*60);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            break;
        }

    }
}