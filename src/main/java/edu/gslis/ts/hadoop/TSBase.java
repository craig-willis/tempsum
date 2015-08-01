package edu.gslis.ts.hadoop;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.TreeMap;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import edu.gslis.textrepresentation.FeatureVector;

public class TSBase extends Configured 
{
    public static Map<String, Integer> readDateBins(String path, FileSystem fs) 
    {
        Map<String, Integer> dateBins = new TreeMap<String, Integer>();
        try
        {
            BufferedReader br = null;
            if (fs == null)  {
                br = new BufferedReader(new FileReader(path));
            } else {
                DataInputStream dis = fs.open(new Path(path));
                br = new BufferedReader(new InputStreamReader(dis));
            }
            String line;
            while ((line = br.readLine()) != null) {
                String[] fields = line.split(" ");
                String date = fields[0];
                int bin = Integer.parseInt(fields[1]);
                dateBins.put(date, bin);                    
            }
            br.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return dateBins;
    }

    public static Map<String, Double> readVocab(String path, FileSystem fs) {
        Map<String, Double> vocab = new TreeMap<String, Double>();
        try
        {
            BufferedReader br = null;
            if (fs == null)  {
                br = new BufferedReader(new FileReader(path));
            } else {
                DataInputStream dis = fs.open(new Path(path));
                br = new BufferedReader(new InputStreamReader(dis));
            }
            String line;
            while ((line = br.readLine()) != null) {
                String[] fields = line.split(" ");
                String term = fields[0];
                double tf = Double.parseDouble(fields[1]);
                //long df = Long.parseLong(fields[2]);
                vocab.put(term, tf);                    
            }
            br.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return vocab;
    }

    
    public static Map<Integer, FeatureVector> readEvents(String path, FileSystem fs) 
    {
        Map<Integer, FeatureVector> queries = new TreeMap<Integer, FeatureVector>();

        try {
            DocumentBuilderFactory dbf = DocumentBuilderFactory
                    .newInstance();
            DocumentBuilder db = dbf.newDocumentBuilder();
            

            Document doc = null;
            if (fs == null) {
                doc = db.parse(new File(path));
            }
            else {
                DataInputStream dis = fs.open(new Path(path));
                doc = db.parse(dis);
            }
            
            NodeList events = doc.getDocumentElement().getElementsByTagName("event");
            for (int i = 0; i < events.getLength(); i++) {
                Node event = events.item(i);
                NodeList elements = event.getChildNodes();
                int id = -1;
                String query = "";

                for (int j = 0; j < elements.getLength(); j++) {
                    Node element = elements.item(j);
                    if (element == null) continue;
                    
                    if (element.getNodeName().equals("id"))
                        id = Integer.parseInt(element.getTextContent());
                    else if (element.getNodeName().equals("query"))
                        query = element.getTextContent();
                }               
                queries.put(id, new FeatureVector(query, null));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return queries;
    }
    
    public static double kl (FeatureVector qv, FeatureVector dv, Map<String, Double> vocab, double mu) {
        double ll = 0;
        
        for (String q: qv.getFeatures()) {
            double df = dv.getFeatureWeight(q);
            double dl = dv.getLength();
            double tf = 1;
            if (vocab.get(q) != null)
                tf = vocab.get(q);
            double cp = tf / vocab.get("TOTAL");
            double pr = (df + mu*cp) / (dl + mu);
            ll += qv.getFeatureWeight(q) * Math.log(pr);
        }
        return ll;
    }
    
    public static double cer(FeatureVector qv, FeatureVector dv, Map<String, Double> vocab, double mu)
    {
        double logLikelihood = 0.0;
        
        for (String q: qv.getFeatures()) {

            double tf = 1;
            if (vocab.get(q) != null)
                tf = vocab.get(q);

            double cp = tf / vocab.get("TOTAL");

            double df = dv.getFeatureWeight(q);
            double dl = dv.getLength();
            double dp = (df + mu * cp) / (dl + mu);

            double qf = qv.getFeatureWeight(q);
            double ql = qv.getLength();
            double qp = qf/ql;

            logLikelihood += qp * Math.log(dp/cp);
        }
        return logLikelihood;
    }
   
}
