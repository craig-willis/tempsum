package edu.gslis.ts.hadoop;

import java.util.Comparator;

public class DocScoreComparator implements Comparator<DocScore> {
    public int compare(DocScore o1, DocScore o2) {
        return o2.getScore().compareTo(o1.getScore());
    }
}