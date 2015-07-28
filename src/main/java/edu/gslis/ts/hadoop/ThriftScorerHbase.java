package edu.gslis.ts.hadoop;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.protocol.TBinaryProtocol;

import streamcorpus_v3.Sentence;
import streamcorpus_v3.Token;
import edu.gslis.streamcorpus.StreamItemWritable;
import edu.gslis.textrepresentation.FeatureVector;


/**
 * Read an HBase table containing serialized thrift entries, order by timestamp.
 */
public class ThriftScorerHbase extends TSBase implements Tool {


    public static class ThriftTableMapper extends TableMapper<Text, Text> 
    {

        double MU = 2500;
        
        Map<Integer, FeatureVector> queries = new TreeMap<Integer, FeatureVector>();
        Map<String, Double> vocab = new TreeMap<String, Double>();

        TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());
        
        Text outputKey = new Text();
        Text outputValue = new Text();
        
        public void map(ImmutableBytesWritable row, Result value, Context context) 
                throws InterruptedException, IOException 
        {
            String streamid = Bytes.toString(row.get());
            int queryId = Bytes.toInt(value.getValue(Bytes.toBytes("cf"), Bytes.toBytes("query")));
            long epoch = Bytes.toLong(value.getValue(Bytes.toBytes("cf"), Bytes.toBytes("epoch")))/1000;
            
            
            StreamItemWritable item = new StreamItemWritable();
            try
            {
                deserializer.deserialize(item, value.getValue(Bytes.toBytes("cf"), Bytes.toBytes("streamitem")));
            } catch (Exception e) {
                e.printStackTrace();
            }
            
            FeatureVector qv = queries.get(queryId);
            
            //String text = item.getBody().getClean_visible();
            String source = item.getSource();
            Map<String, List<Sentence>> parsers = item.getBody().getSentences();
            List<Sentence> sentenceParser = parsers.get("lingpipe");
            
            List<Double> sentenceScores = new ArrayList<Double>();
            List<String> sentences = new ArrayList<String>();
            String docText = "";
            if (sentenceParser != null && sentenceParser.size() > 0) 
            {

                for (Sentence s: sentenceParser) {
                    try {
                       List<Token> tokens = s.tokens;
                       String sentence = "";
                       for (Token token: tokens) {
                           String tok = token.token;
                           sentence += tok + " ";
                       }
                       FeatureVector sv = new FeatureVector(sentence, null);
                       double score = kl(qv, sv, vocab, MU);

                       sentences.add(sentence);
                       sentenceScores.add(score);
                       
                       docText += sentence + "\n";
                    } catch (Exception e) {
                        e.printStackTrace();
                        System.err.println("Issue with sentence "+sentences.size()+" in doc "+streamid);
                    }
                }
                FeatureVector dv = new FeatureVector(docText, null);
                double docScore = kl(qv, dv, vocab, MU);

                int sentIdx = 0;
                String topText = "";
                for (String sentence: sentences) {
                    if (sentIdx < 20) {
                        topText += sentence + "\n";                        
                    }
                    sentIdx ++;
                }
                
                FeatureVector tv = new FeatureVector(topText, null);
                double topScore = kl(qv, tv, vocab, MU);
                
                outputKey.set(String.valueOf(queryId));
                int sentNum = 0;
                double[] minmax = getMinMax(qv, dv.getLength(), MU);
                
                for (double sentScore: sentenceScores) 
                {
                    outputValue.set( epoch + "," + source + "," + streamid + "," 
                                + sentNum + "," + docScore + "," + topScore + "," + sentScore + "," + sentenceScores.size() 
                                + "," + dv.getLength() + "," + minmax[0] + "," + minmax[1] + "," + minmax[2]);
                    
                    context.write(outputKey, outputValue);

                    sentNum ++;
                }
                                
            } else if (sentenceParser == null) {
                System.err.println("Sentence parser null " + streamid);
            } else if (sentenceParser.size() == 0) {
                System.err.println("Sentence length 0 " + streamid);
            } else {
                System.err.println("Other sentence error "  + streamid);
            }
        }
        
        
        protected double[] getMinMax(FeatureVector qv, double docLen, double mu) 
        {
            double[] minmax = new double[3];
            
            double minll = 0;
            double maxP = Double.NEGATIVE_INFINITY;
            String maxT = "";
            for (String q: qv.getFeatures()) {
                double tf = vocab.containsKey(q) ? vocab.get(q) : 1;
                double total = vocab.get("TOTAL");
                double cp = tf/total;
                
                if (cp > maxP) {
                    maxP = cp;
                    maxT = q;
                }
                double df = 0;
                double pr = (df + mu*cp) / (docLen + mu);
                minll += qv.getFeatureWeight(q) * Math.log(pr);                
            }
            
            double maxll = 0;                   
            for (int i=0; i<qv.getLength(); i++) 
            {
                double tf = vocab.get(maxT);
                double total = vocab.get("TOTAL");
                double cp = tf/total;
                double df = docLen;
                double pr = (df + mu*cp) / (docLen + mu);
                maxll += 1 * Math.log(pr);                
            }
                
            double pracll = 0;
            for (String q: qv.getFeatures())
            {
                double tf = vocab.containsKey(q) ? vocab.get(q) : 1;
                double total = vocab.get("TOTAL");
                double cp = tf/total;
                
                double df = 10;
                double pr = (df + mu*cp) / (docLen + mu);
                pracll += qv.getFeatureWeight(q) * Math.log(pr);    
            }
            minmax[0] = minll;
            minmax[1] = maxll;
            minmax[2] = pracll;
            return minmax;

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
                    }
                } else {
                    Path[] paths = context.getLocalCacheFiles();
                    for (Path path : paths) {
                        if (path.toString().contains("topics"))
                            queries = readEvents(path.toString(), null);
                        if (path.toString().contains("vocab"))
                            vocab = readVocab(path.toString(), null);
                    }
                }
            } catch (Exception ioe) {
                ioe.printStackTrace();
            }
        }
    }
    

    public static class ThriftTableReducer extends Reducer<Text, Text, Text, Text> 
    {
        public void reduce(Text key, Iterable<Text> values, Context context) 
                throws IOException, InterruptedException 
        {
            // Sort the values by epoch
            
            Map<String, Map<String, Object>> temp = new TreeMap<String, Map<String, Object>>();
            for (Text value: values) {
                // 1330610340,news,1330610340-ef8a0109512723e361c89ff664cd0189,9,-36.183868182678786,-36.183868182678786,-42.57892681491056,10
                String[] fields = value.toString().split(",");
                long epoch = Long.parseLong(fields[0]);
                String source = fields[1];
                String streamid = fields[2];
                int sentNum = Integer.parseInt(fields[3]);
                double docScore = Double.parseDouble(fields[4]);
                double topScore = Double.parseDouble(fields[5]);
                double sentScore = Double.parseDouble(fields[6]);
                double numSent = Integer.parseInt(fields[7]);
                
                Map<String, Object> vals = new HashMap<String, Object>();
                vals.put("source", source);
                vals.put("sentNum", sentNum);
                vals.put("docScore", docScore);
                vals.put("topScore", topScore);
                vals.put("sentScore", sentScore);
                vals.put("numSent", numSent);
                
                temp.put(streamid, vals);
            }
            
            for (String streamid: temp.keySet()) {
                System.out.println(streamid);
            }
        }
    }

    public int run(String[] args) throws Exception 
    {
        String tableName = args[0];
        //int queryId = Integer.parseInt(args[1]);
        Path topicsFile = new Path(args[1]);
        Path vocabFile = new Path(args[2]);
        Path outputPath = new Path(args[3]);
        
        // http://hbase.apache.org/0.94/book/mapreduce.example.html
        Configuration config = HBaseConfiguration.create(getConf());
        Job job = Job.getInstance(config);
        job.setJarByClass(ThriftScorerHbase.class);
        
        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        
        /*
        Filter filter = new SingleColumnValueFilter(
                Bytes.toBytes("cf"), Bytes.toBytes("query"),
                CompareOp.EQUAL,
                Bytes.toBytes(queryId)
        );
        scan.setFilter(filter);
        */
        
        TableMapReduceUtil.initTableMapperJob(
                tableName,                  // input HBase table name
                scan,                       // Scan instance to control CF and attribute selection
                ThriftTableMapper.class,    // mapper
                Text.class,                 // mapper output key
                Text.class,                 // mapper output value
                job
        );
        
        job.addCacheFile(topicsFile.toUri());
        job.addCacheFile(vocabFile.toUri());
        
        FileOutputFormat.setOutputPath(job, outputPath);

        
        boolean b = job.waitForCompletion(true);
        if (!b) {
            throw new IOException("error with job!");
        }
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new ThriftScorerHbase(),
                args);
        System.exit(res);
    }
}
