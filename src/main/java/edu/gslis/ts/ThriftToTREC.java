package edu.gslis.ts;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
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


/**
 * Score documents in thrift files w.r.t. temporal summarization queries.
 */
public class ThriftToTREC 
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
        	String outfile = cmd.getOptionValue("o");
        	String sentenceParser = cmd.getOptionValue("p");
        	
        	// Setup the filter
	    	ThriftToTREC f = new ThriftToTREC();
	    	
	    	if (in != null && outfile != null) {
	    		File infile = new File(in);
	    		if (infile.isDirectory()) {
	    			for (File file: infile.listFiles()) {
	    				if (file.isDirectory()) {
	    					for (File filefile : file.listFiles()) {
	    						f.filter(filefile, new File(outfile), sentenceParser);
	    					}
	    				} else {
	    					f.filter(file, new File(outfile), sentenceParser);
	    				}
	    			}
	    		}
	    		else
	    			f.filter(infile, new File(outfile), sentenceParser);
	    	}
	    
    	} catch (Exception e) {
    		e.printStackTrace();
    	}
	}

	public static Options createOptions()
	{
		Options options = new Options();
		options.addOption("i", true, "Input thrift file");
		options.addOption("o", true, "Output file");
		options.addOption("p", true, "Parser (lingpipe or serif)");
		return options;
	}
	
	/**
	 * @param thriftFile
	 */
	public Map<String, String> filter(File infile, File outfile, String parser) 
	{
		Map<String, String> results = new TreeMap<String, String>();
		try
		{	
			InputStream in = null;
			
			if (infile.getName().endsWith(".gz")) 
				in = new GZIPInputStream(new FileInputStream(infile));
			else if (infile.getName().endsWith("xz"))
			    in = new XZInputStream(new FileInputStream(infile));
			else
				in = new FileInputStream(infile);
			
	        TTransport inTransport = 
	        	new TIOStreamTransport(new BufferedInputStream(in));
	        TBinaryProtocol inProtocol = new TBinaryProtocol(inTransport);
	        inTransport.open();
	        
            OutputStreamWriter out = 
                	new OutputStreamWriter(new FileOutputStream(outfile, false), 
                			"UTF-8");
            try 
	        {
                Charset charset = Charset.forName("UTF-8");
                CharsetDecoder decoder = charset.newDecoder();

                // Run through items in the thrift file
	            while (true) 
	            {
	                final StreamItem item = new StreamItem();
	                item.read(inProtocol);
	                if (item.body == null || item.body.clean_visible == null) {
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
	                
	                String url = "";
	                if (item.abs_url != null) {
	                	url = decoder.decode(item.abs_url).toString();
	                }
	                	                
	                Map<String, List<Sentence>> parsers = item.body.sentences;
                    List<Sentence> sentenceParser = parsers.get(parser);

	                String sentencesText = "";
	                int sentenceNum = 0;
	                if (sentenceParser != null && sentenceParser.size() > 0) {

                        for (Sentence s: sentenceParser) {
                           List<Token> tokens = s.tokens;
                           String sentence = "";
                           for (Token token: tokens) {
                               String tok = token.token;
                               sentence += tok + " ";
                           }
                           sentencesText += sentenceNum + " " + sentence + "\n";
                           sentenceNum++;
                        }
	                }
	                
	                
                    try
                    {
                        String hourDayDir = outfile.getName().replace(".txt", "");
                        out.write("<DOC>\n");
                        out.write("<DOCNO>" + streamId + "</DOCNO>\n");
                        out.write("<SOURCE>" + source + "</SOURCE>\n");
                        out.write("<URL>" + url + "</URL>\n");
                        out.write("<DATETIME>" + dateTime + "</DATETIME>\n");
                        out.write("<HOURDAYDIR>" + hourDayDir + "</HOURDAYDIR>\n");
                        out.write("<EPOCH>" + epochTime + "</EPOCH>\n");
                        out.write("<TEXT>\n" + sentencesText + "\n</TEXT>\n");
                        out.write("</DOC>\n");
                    } catch (Exception e) {
                        System.out.println("Error processing " + infile.getAbsolutePath() + " " + item.stream_id);
                        e.printStackTrace();
                    }
	                
	            }   
	        } catch (TTransportException te) {
	            if (te.getType() == TTransportException.END_OF_FILE) {
	            } else {
	                throw te;
	            }
	        }
	        inTransport.close();
	        out.close();
	        
	    } catch (Exception e) {
	    	System.out.println("Error processing " + infile.getName());
	        e.printStackTrace();
	    }
	    return results;		
	}
}
