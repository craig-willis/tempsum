package edu.gslis.ts;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.tukaani.xz.XZInputStream;

import streamcorpus_v3.Sentence;
import streamcorpus_v3.StreamItem;
import streamcorpus_v3.Token;
import edu.gslis.docscoring.QueryDocScorer;
import edu.gslis.docscoring.ScorerDirichlet;
import edu.gslis.docscoring.support.CollectionStats;
import edu.gslis.docscoring.support.IndexBackedCollectionStats;
import edu.gslis.queries.GQuery;
import edu.gslis.searchhits.SearchHit;
import edu.gslis.textrepresentation.FeatureVector;


/**
 * Score documents in thrift files w.r.t. temporal summarization queries.
 */
public class DumpThriftData 
{

	public static void main(String[] args) 
	{
    	try
    	{
    		// Get the commandline options
    		Options options = createOptions();
    		CommandLineParser parser = new GnuParser();
    		CommandLine cmd = parser.parse( options, args);
        	
        	String in = cmd.getOptionValue("i");
        	String sentenceParser = cmd.getOptionValue("p");
        	String query = cmd.getOptionValue("q");
        	String externalCollection = cmd.getOptionValue("e");
        	
        	// Get background statistics
        	CollectionStats bgstats = new IndexBackedCollectionStats();
        	bgstats.setStatSource(externalCollection);
        	
        	// Set query
        	GQuery gquery = new GQuery();
        	gquery.setText(query);
        	gquery.setFeatureVector(new FeatureVector(query, null));
        	
        	// Setup the filter
	    	DumpThriftData f = new DumpThriftData();
	    	
	    	if (in != null) {
	    		File infile = new File(in);
	    		if (infile.isDirectory()) {
	    			for (File file: infile.listFiles()) {
	    				if (file.isDirectory()) {
	    					for (File filefile : file.listFiles()) {
	    						System.err.println(filefile.getAbsolutePath());
	    						f.filter(filefile, sentenceParser, gquery, bgstats);
	    					}
	    				} else {
	    					System.err.println(file.getAbsolutePath());
	    					f.filter(file, sentenceParser, gquery, bgstats);
	    				}
	    			}
	    		}
	    		else
	    			System.err.println(infile.getAbsolutePath());
	    			f.filter(infile, sentenceParser, gquery, bgstats);
	    	}
	    
    	} catch (Exception e) {
    		e.printStackTrace();
    	}
	}

	public static Options createOptions()
	{
		Options options = new Options();
		options.addOption("i", true, "Input thrift file");
		options.addOption("p", true, "Parser (lingpipe or serif)");
		options.addOption("q", true, "Query");
		options.addOption("e", true, "External Collection");
		return options;
	}
	
	/**
	 * @param thriftFile
	 */
	public void filter(File infile, String parser, GQuery gquery, CollectionStats bgstats) 
	{
		try
		{	
			InputStream in = null;
			
			if (infile.getName().endsWith(".gz")) 
				in = new GZIPInputStream(new FileInputStream(infile));
			else if (infile.getName().endsWith("xz"))
			    in = new XZInputStream(new FileInputStream(infile));
			else {
				System.err.println("Regular FileInputStream");
				in = new FileInputStream(infile);
			}
			
	        TTransport inTransport = 
	        	new TIOStreamTransport(new BufferedInputStream(in));
	        TBinaryProtocol inProtocol = new TBinaryProtocol(inTransport);
	        inTransport.open();
	        
            try 
	        {
                // Run through items in the thrift file
	            while (true) 
	            {
	                final StreamItem item = new StreamItem();
	                item.read(inProtocol);
	                if (item.body == null || item.body.clean_visible == null) {
	                	System.err.println("Body is null.");
	                	continue;
	                }
	                
	                String streamId = "";
	                if (item.stream_id != null) {
	                	streamId = item.stream_id;
	                }
	                	                
	                String dateTime = "";
	                long epochTime = 0;
	                if (item.stream_time != null && item.stream_time.zulu_timestamp != null)
	                {
		                dateTime = item.stream_time.zulu_timestamp;
		                DateTimeFormatter dtf = ISODateTimeFormat.dateTime();
		                epochTime = dtf.parseMillis(dateTime);	
	                }
	                
	                String source = "";
	                if (item.source != null) {
	                	source = item.source;
	                }
	                	                
	                Map<String, List<Sentence>> parsers = item.body.sentences;
                    List<Sentence> sentenceParser = parsers.get(parser);

                    QueryDocScorer scorer = new ScorerDirichlet();
                    scorer.setCollectionStats(bgstats);
                    scorer.setQuery(gquery);
                    
                    List<Double> sentenceScores = new ArrayList<Double>();
                    List<String> sentences = new ArrayList<String>();
	                String sentencesText = "";
	                if (sentenceParser != null && sentenceParser.size() > 0) {

                        for (Sentence s: sentenceParser) {
                        	try {
	                           List<Token> tokens = s.tokens;
	                           String sentence = "";
	                           for (Token token: tokens) {
	                               String tok = token.token;
	                               sentence += tok + " ";
	                           }
	                           FeatureVector sentenceVector = new FeatureVector(sentence, null);
	                           SearchHit sentenceHit = new SearchHit();
	                           sentenceHit.setFeatureVector(sentenceVector);
	                           sentenceHit.setLength(sentenceVector.getLength());
	                           double score = scorer.score(sentenceHit);

	                           sentenceScores.add(score);
	                           sentences.add(sentence);
	                           
	                           sentencesText += sentence + "\n";
                        	} catch (Exception e) {
                        		System.err.println("Issue with sentence "+sentences.size()+" in doc "+streamId);
                        		System.err.println("File: "+infile.getAbsolutePath());
                        	}
                        }
                        SearchHit docHit = new SearchHit();
                        docHit.setFeatureVector(new FeatureVector(sentencesText, null));
                        double docscore = scorer.score(docHit);
                        for (int i = 0; i < sentenceScores.size(); i++) {
                        	System.out.println(infile.getAbsolutePath()+"\t"+source+"\t"+epochTime+"\t"+streamId+"\t"+docscore+"\t"+i+"\t"+sentenceScores.get(i)+"\t"+sentences.get(i));
                        }
	                } else if (sentenceParser == null) {
	                	System.err.println("Sentence parser null");
	                } else if (sentenceParser.size() == 0) {
	                	System.err.println("Sentence length 0");
	                } else {
	                	System.err.println("Other sentence error.");
	                }
	                
	            }   
	        } catch (TTransportException te) {
	            if (te.getType() == TTransportException.END_OF_FILE) {
	            } else {
	                throw te;
	            }
	        }
            
	        inTransport.close();
	        
	    } catch (Exception e) {
	    	System.err.println("Error processing " + infile.getAbsolutePath()+ " " + infile.getName());
	        e.printStackTrace();
	    }
	}
}
