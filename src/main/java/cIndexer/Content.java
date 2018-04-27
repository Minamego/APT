package cIndexer;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

public class Content {
    public int last = -1;
    public ArrayList<Integer> urls = new ArrayList<Integer>();
    public ArrayList<ArrayList<Integer>> pos = new ArrayList<ArrayList<Integer>>();
    public ArrayList<ArrayList<Integer>> tags = new ArrayList<ArrayList<Integer>>();
    public ArrayList<Integer> curPos;
    public ArrayList<Integer> curTags;
    public void add(Integer url , int idx , int p , int t) {
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

    public void addLast()
    {
        if(last != -1)
        {
            pos.add(curPos);
            tags.add(curTags);
        }

    }
}