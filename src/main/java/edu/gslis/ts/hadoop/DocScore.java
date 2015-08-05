package edu.gslis.ts.hadoop;

public class DocScore 
{
    String docid;
    double score;
    
    public DocScore(String docid, double score) {
        this.docid = docid;
        this.score = score;
    }
    public String getDocId() {
        return docid;
    }
    public void setDocId(String docid) {
        this.docid = docid;
    }
    public Double getScore() {
        return score;
    }
    public void setScore(double score) {
        this.score = score;
    }
}
