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

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.protocol.TBinaryProtocol;

import edu.gslis.searchhits.SearchHit;
import edu.gslis.streamcorpus.StreamItemWritable;
import edu.gslis.textrepresentation.FeatureVector;
import edu.gslis.utils.Stopper;


/**
 * Read an HBase table containing serialized thrift entries
 */
public class ThriftRMScorerHbaseMR extends TSBase implements Tool {

    static double MU = 2500;
    static int numFbDocs = 20;
    static int numFbTerms = 20;
    static double rmLambda = 0.5;
    static int numDocs = 1000;
    
    public static class ThriftTableMapper extends TableMapper<IntWritable, Text> 
    {

        
        Map<Integer, FeatureVector> queries = new TreeMap<Integer, FeatureVector>();
        Map<String, Double> vocab = new TreeMap<String, Double>();
        Stopper stopper = new Stopper();


        TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());
        
        IntWritable outputKey = new IntWritable();
        Text outputValue = new Text();
        int queryId;
        long epoch;
        String rowkey;
        StreamItemWritable item = new StreamItemWritable();
        FeatureVector qv;
        
        public void map(ImmutableBytesWritable row, Result value, Context context) 
                throws InterruptedException, IOException 
        {
            rowkey = Bytes.toString(row.get());
            queryId = Bytes.toInt(value.getValue(Bytes.toBytes("md"), Bytes.toBytes("query")));
            epoch = Bytes.toLong(value.getValue(Bytes.toBytes("md"), Bytes.toBytes("epoch")))/1000;
            
            
            try
            {
                deserializer.deserialize(item, value.getValue(Bytes.toBytes("si"), Bytes.toBytes("streamitem")));
            } catch (Exception e) {
                e.printStackTrace();
            }
            
            qv = queries.get(queryId);
            
            String docText = item.getBody().getClean_visible();

            FeatureVector dv = new FeatureVector(docText, stopper);
            double docScore = kl(qv, dv, vocab, MU);
            
            outputKey.set(queryId);
            outputValue.set(rowkey + "," + docScore);
            context.write(outputKey, outputValue);

        }       

        
        public static double maxest (FeatureVector qv, FeatureVector dv, Map<String, Double> vocab, double mu) 
        {
            double ll = 0;
 
            double dl = dv.getLength();
            
            for (String q: qv.getFeatures()) {
                double tf = 1;
                if (vocab.get(q) != null)
                    tf = vocab.get(q);
                double cp = tf / vocab.get("TOTAL");
                double pr = (1 + mu*cp) / (dl + mu);
                ll += qv.getFeatureWeight(q) * Math.log(pr);
            }
            return ll;
        }
        
        
        protected void setup(Context context) {
            try {
                // Side-load the topics
                URI[] files = context.getCacheFiles();
                if (files != null) {
                    FileSystem fs = FileSystem.get(context.getConfiguration());

                    for (URI file : files) {
                        if (file.toString().contains("topics"))
                            queries = readEvents(file.toString(), fs);
                        if (file.toString().contains("vocab"))
                            vocab = readVocab(file.toString(), fs);
                        if (file.toString().contains("stop"))
                            stopper = readStoplist(file.toString(), fs);                        
                    }
                } else {
                    Path[] paths = context.getLocalCacheFiles();
                    for (Path path : paths) {
                        if (path.toString().contains("topics"))
                            queries = readEvents(path.toString(), null);
                        if (path.toString().contains("vocab"))
                            vocab = readVocab(path.toString(), null);
                        if (path.toString().contains("stop"))
                            stopper = readStoplist(path.toString(), null);                        
                    }
                }
            } catch (Exception ioe) {
                ioe.printStackTrace();
            }
        }
    }
    

    public static class ThriftTableReducer extends Reducer<IntWritable, Text, IntWritable, Text> 
    {
        Table table;
        TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());
        Map<Integer, FeatureVector> queries = new TreeMap<Integer, FeatureVector>();
        Map<String, Double> vocab = new TreeMap<String, Double>();
        Stopper stopper = new Stopper();
        Text outputValue = new Text();
        
        public void reduce(IntWritable queryId, Iterable<Text> values, Context context) 
                throws IOException, InterruptedException 
        {
            
            List<DocScore> docScores = new ArrayList<DocScore>();
            List<String> rowKeys = new ArrayList<String>();
            for (Text value: values) {
                // Stream id, score
                String[] fields = value.toString().split(",");
                String rowkey = fields[0];
                rowKeys.add(rowkey);
                double score = Double.parseDouble(fields[1]);
                DocScore docScore = new DocScore(rowkey, score);
                docScores.add(docScore);
            }        
            
            Collections.sort(docScores, new DocScoreComparator());
                     
            docScores = docScores.subList(0, 1000);
            FeatureVector qv = queries.get(queryId.get());

            FeatureVector rm = buildRM3Model(docScores, numFbDocs, numFbTerms, qv, rmLambda);
            
            List<DocScore> rmDocScores = new ArrayList<DocScore>();

            for (DocScore ds: docScores) {
//            for (String rowkey: rowKeys) {
                // Stream id, score
                String rowkey = ds.getDocId();
                String[] keyfields = rowkey.split("\\.");
                String streamid = keyfields[1];
                FeatureVector dv = getDocVector(rowkey);
                double score = kl(rm, dv, vocab, MU);
                DocScore rmDs = new DocScore(streamid, score);
                rmDocScores.add(rmDs);
            }            
            
            Collections.sort(rmDocScores, new DocScoreComparator());
            
            for (DocScore ds: rmDocScores) {
                System.out.println(ds.getDocId() + "," + ds.getScore());
                outputValue.set(ds.getDocId() + "," + ds.getScore());
                context.write(queryId, outputValue);
            }
            
        } 
        
        protected void setup(Context context) {
            try
            {
                Configuration config = HBaseConfiguration.create(context.getConfiguration());
                Connection connection = ConnectionFactory.createConnection(config);

                table = connection.getTable(TableName.valueOf(config.get("hbase.table.name")));
                
                try {
                    // Side-load the topics
                    URI[] files = context.getCacheFiles();
                    if (files != null) {
                        FileSystem fs = FileSystem.get(context.getConfiguration());

                        for (URI file : files) {
                            if (file.toString().contains("topics"))
                                queries = readEvents(file.toString(), fs);
                            if (file.toString().contains("vocab"))
                                vocab = readVocab(file.toString(), fs);
                            if (file.toString().contains("stop"))
                                stopper = readStoplist(file.toString(), fs);
                        }
                    } else {
                        Path[] paths = context.getLocalCacheFiles();
                        for (Path path : paths) {
                            if (path.toString().contains("topics"))
                                queries = readEvents(path.toString(), null);
                            if (path.toString().contains("vocab"))
                                vocab = readVocab(path.toString(), null);
                            if (path.toString().contains("stop"))
                                stopper = readStoplist(path.toString(), null);                            
                        }
                    }
                } catch (Exception ioe) {
                    ioe.printStackTrace();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
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
        
        public FeatureVector buildRM3Model(List<DocScore> docScores, int fbDocCount, int numFbTerms, FeatureVector qv,
                double lambda) 
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
                
                FeatureVector docVector = getDocVector(ds.getDocId());
                
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
    }
    
    public int run(String[] args) throws Exception 
    {
            String tableName = args[0];
            Path topicsFile = new Path(args[1]);
            Path vocabFile = new Path(args[2]);
            Path outputPath = new Path(args[3]);
            Path stoplist = new Path(args[4]);
        // String queryId = args[1];
        
        Configuration config = HBaseConfiguration.create(getConf());
        config.set("hbase.table.name", tableName);
        Job job = Job.getInstance(config);
        job.setJarByClass(ThriftRMScorerHbaseMR.class);
        
        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        /*
        Filter prefixFilter = new PrefixFilter(Bytes.toBytes(queryId));
        scan.setFilter(prefixFilter);
        */
        
        TableMapReduceUtil.initTableMapperJob(
                tableName,
                scan, 
                ThriftTableMapper.class, 
                IntWritable.class, // mapper output key
                Text.class, // mapper output value
                job
        );
        
        job.setReducerClass(ThriftTableReducer.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        
        job.addCacheFile(topicsFile.toUri());
        job.addCacheFile(vocabFile.toUri());
        job.addCacheFile(stoplist.toUri());
        FileOutputFormat.setOutputPath(job, outputPath);

        
        boolean b = job.waitForCompletion(true);
        if (!b) {
            throw new IOException("error with job!");
        }
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new ThriftRMScorerHbaseMR(),
                args);
        System.exit(res);
    }
}
