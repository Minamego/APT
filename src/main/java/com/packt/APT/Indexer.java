package com.packt.APT;

import java.io.*;
import java.util.*;

/**
 * Hello world!
 */
public class Indexer {
    private static Map<String, Boolean> isStopWord;

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

    /*
    take the output from crawler and iterate on every word and if it isn't stop word stem it
    when you finish iterate on every word and push it into database
    */
    class Content
    {
        int last = -1;
        public Set<String> urls = new HashSet<String>();
        public ArrayList<ArrayList<Integer>> pos = new ArrayList<ArrayList<Integer>>();
        public ArrayList<ArrayList<Integer>> tags = new ArrayList<ArrayList<Integer>>();
        public ArrayList<Integer> curPos;
        public ArrayList<Integer> curTags;
        public void add(String url , int idx , int p , int t) {
            if(last != idx)
            {
                if(last != -1)
                {
                    pos.add(curPos);
                    tags.add(curTags);
                }
                urls.add(url);
                curPos = new ArrayList<Integer>();
                curTags = new ArrayList<Integer>();
                last = idx;
            }
            curPos.add(p);
            curTags.add(t);
        }
    }
    private static void index( ArrayList<String> urls, ArrayList<ArrayList<String>> data  , ArrayList<ArrayList<Integer>> tags ,dbConnector db , Stem stemmer) {
        Map<String , Content>terms = new HashMap<String, Content>();
        /*
        loop on the text from crawler
         */



        /*************/
        for(Map.Entry<String , Content> term : terms.entrySet())
        {
            Content c = term.getValue();
            db.insertTerm(term.getKey() , c.urls , c.pos , c.tags);
        }
    }

    public static void main(String[] args) throws IOException {
        readStopWords();
        dbConnector db = new dbConnector();
        Stem stemmer = new Stem();
        //db.clean();
        //db.printDocs();
        db.close();
    }
}
