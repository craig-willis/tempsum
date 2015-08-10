/*******************************************************************************
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package edu.gslis.ts.hadoop;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.protocol.TBinaryProtocol;

import edu.gslis.streamcorpus.StreamItemWritable;
import edu.gslis.textrepresentation.FeatureVector;
import edu.gslis.utils.Stopper;


/**
 * Read an HBase table containing serialized thrift entries
 */
public class ThriftRMScorerHbase extends TSBase
{

    static double MU = 2500;
    static int numFbDocs = 20;
    static int numFbTerms = 20;
    static double rmLambda = 0.5;

    Map<Integer, FeatureVector> queries;
    Map<String, Double> vocab;
    Stopper stopper;
    Configuration config;
    Connection connection;
    Table table;
    TDeserializer deserializer;
    String tableName;
    int queryId;
    int scanSize;
    
    public static void main(String[] args) throws Exception 
    {
        String tableName = args[0];
        String topicsFile = args[1];
        String vocabFile = args[2];
        String outputPath = args[3];        
        String stoplist = args[4];
        int queryId = Integer.parseInt(args[5]);
        int scanSize = Integer.parseInt(args[6]);
        
        ThriftRMScorerHbase scorer = new ThriftRMScorerHbase(tableName, topicsFile, vocabFile,
                outputPath, stoplist, queryId, scanSize);
        scorer.doit();
    }
    
    public ThriftRMScorerHbase(String tableName, String topicsFile, String vocabFile, String
            outputPath, String stoplist, int queryId, int scanSize) throws Exception
    {
        
        this.tableName = tableName;
        this.queryId = queryId;
        queries = readEvents(topicsFile, null);
        vocab = readVocab(vocabFile, null);
        stopper = readStoplist(stoplist, null); 
        this.scanSize = scanSize;

        config = HBaseConfiguration.create();
        int timeout = 60000*20;
        config.set("hbase.rpc.timeout", String.valueOf(timeout));
        connection = ConnectionFactory.createConnection(config);

        table = connection.getTable(TableName.valueOf(tableName));
        deserializer = new TDeserializer(new TBinaryProtocol.Factory());
        
    }
    
    public void doit() throws Exception
    {
        
        String queryStr = String.format("%02d", queryId);

//        System.err.println("Scanning table " + tableName + " for query " + queryStr);
//        Scan s = new Scan();
   
//       Scan scan = new Scan(Bytes.toBytes("a.b.x|1"),Bytes.toBytes("a.b.x|2"));

//        s.setCaching(scanSize);
//        s.setLoadColumnFamiliesOnDemand(true);
//        s.setBatch(scanSize);
//        s.addColumn(Bytes.toBytes("md"), Bytes.toBytes("query"));
        
//        Filter prefixFilter = new PrefixFilter(Bytes.toBytes(queryStr));
//        s.setFilter(prefixFilter);

//        ResultScanner scanner = table.getScanner(s);

        FeatureVector qv = queries.get(queryId);
        
        TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());

        Map<String, FeatureVector> docVectors = new HashMap<String, FeatureVector>();
        List<DocScore> docScores = new ArrayList<DocScore>();
        
        System.err.println("Scoring streamitems");
        
        for (int bin=0; bin<3000; bin+=scanSize)
        {
            String startBin = queryStr + String.format("%04d", bin);
            String endBin = queryStr + String.format("%04d", bin +scanSize);
            
            System.err.println("\nScanning table " + tableName + " for rows " + startBin + "-" + endBin);
            
            Scan scan = new Scan(Bytes.toBytes(startBin),Bytes.toBytes(endBin));

            ResultScanner scanner = table.getScanner(scan);
            try 
            {
                
                int i=0;
                //for (Result[] rrs = scanner.next(scanSize); rrs != null; rrs = scanner.next(scanSize)) 
                for (Result rr = scanner.next(); rr != null; rr = scanner.next()) 
                {
                    
                    //for (Result rr: rrs) {
                        String rowkey = Bytes.toString(rr.getRow());
        
                        StreamItemWritable item = new StreamItemWritable();
                        
                        if (i % 1000 == 0)
                            System.err.print(".");
                        try     
                        {
                            deserializer.deserialize(item, rr.getValue(Bytes.toBytes("si"), Bytes.toBytes("streamitem")));
                            String docText = item.getBody().getClean_visible();
        
                            FeatureVector dv = new FeatureVector(docText, stopper);
                            docVectors.put(rowkey, dv);
                            double score = kl(qv, dv, vocab, MU);
                            
                            DocScore ds = new DocScore(rowkey, score);
                            docScores.add(ds);
                            i++;
                        }
                        catch (Exception e) {
                            e.printStackTrace();
                        }  
                    //}
                }
    
              } finally {
                scanner.close();
              }
        }
            
        System.err.println("Sorting");
        Collections.sort(docScores, new DocScoreComparator());
                    

        System.err.println("Building RM model");
        FeatureVector rm = buildRM3Model(docScores, numFbDocs, numFbTerms, qv, rmLambda, stopper, docVectors);
        
        List<DocScore> rmDocScores = new ArrayList<DocScore>();

        System.err.println("Rescoring RM model");
        int i=0;
        for (DocScore ds: docScores) {
            if (i % 1000 == 0)                     
                System.err.print(".");

            // Stream id, score
            String rowkey = ds.getDocId();
            String[] keyfields = rowkey.split("\\.");
            String streamid = keyfields[1];
            FeatureVector dv = docVectors.get(rowkey); //getDocVector(rowkey);
            double score = kl(rm, dv, vocab, MU);
            
            DocScore rmds = new DocScore(streamid, score);
            rmDocScores.add(rmds);
            i++;
        }            
        
        System.err.println("Sorting");
        Collections.sort(rmDocScores, new DocScoreComparator());
        
        for (DocScore ds: rmDocScores) {
            System.out.println(queryId + "," + ds.getDocId() + "," + ds.getScore());
        }
        

    }
    
    
    public FeatureVector buildRM3Model(List<DocScore> docScores, int fbDocCount, int numFbTerms, FeatureVector qv,
            double lambda, Stopper stopper, Map<String, FeatureVector> docVectors) 
    {
        Set<String> vocab = new HashSet<String>();
        List<FeatureVector> fbDocVectors = new LinkedList<FeatureVector>();
        FeatureVector model = new FeatureVector(stopper);

        if (docScores.size() < fbDocCount)
            fbDocCount = docScores.size();
            
        double[] rsvs = new double[docScores.size()];
        int k=0;
        for (int i=0; i<fbDocCount; i++) 
        {
            DocScore ds = docScores.get(i);
            rsvs[k++] = Math.exp(ds.getScore());
            
            //FeatureVector docVector = getDocVector(ds.getDocId());
            FeatureVector docVector = docVectors.get(ds.getDocId());
            if (docVector != null) {
                vocab.addAll(docVector.getFeatures());
                fbDocVectors.add(docVector);
            }
        }
        
        Iterator<String> it = vocab.iterator();
        while(it.hasNext()) 
        {
            String term = it.next();

            double fbWeight = 0.0;

            Iterator<FeatureVector> docIT = fbDocVectors.iterator();
            k=0;
            while(docIT.hasNext()) {
                FeatureVector docVector = docIT.next();
                double docProb = docVector.getFeatureWeight(term) / docVector.getLength();
                double docWeight = 1.0;
                docProb *= rsvs[k++];
                docProb *= docWeight;
                fbWeight += docProb;
            }
            
            fbWeight /= (double)fbDocVectors.size();
            
            model.addTerm(term, fbWeight);
        }
        model.clip(numFbTerms);
        model.normalize();
        
        model = FeatureVector.interpolate(qv, model, lambda);
        return model;
    }  
    
    public FeatureVector getDocVector(String rowKey)
    {
        try {
            Get g = new Get(Bytes.toBytes(rowKey));
            Result r = table.get(g);
            
            StreamItemWritable item = new StreamItemWritable();

            
            try
            {
                deserializer.deserialize(item, r.getValue(Bytes.toBytes("si"), Bytes.toBytes("streamitem")));
            } catch (Exception e) {
                System.out.println("Error getting row: " + rowKey);
                e.printStackTrace();
            }
            
            String docText = item.getBody().getClean_visible();

            FeatureVector dv = new FeatureVector(docText, stopper);

            return dv;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new FeatureVector(stopper);
    }
}
