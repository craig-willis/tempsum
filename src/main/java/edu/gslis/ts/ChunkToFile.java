package edu.gslis.ts;
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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.zip.GZIPInputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.io.FileUtils;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.tukaani.xz.LZMA2Options;
import org.tukaani.xz.XZInputStream;
import org.tukaani.xz.XZOutputStream;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import streamcorpus_v3.StreamItem;
import edu.gslis.indexes.IndexWrapper;
import edu.gslis.indexes.IndexWrapperFactory;
import edu.gslis.textrepresentation.FeatureVector;
import edu.gslis.utils.Stopper;
import gov.loc.repository.pairtree.Pairtree;


/**
 * Given a compressed chunk file, write out individual thrift files to the filesystem.
 * 
 */
public class ChunkToFile 
{
    final static double MU = 2500;

	public static void main(String[] args) 
	{
    	try
    	{
    		// Get the commandline options
    		Options options = createOptions();
    		CommandLineParser parser = new GnuParser();
    		CommandLine cmd = parser.parse( options, args);
        	
        	String inputPath = cmd.getOptionValue("input");
            String indexPath = cmd.getOptionValue("index");
            String eventsPath = cmd.getOptionValue("events");
            String stopPath = cmd.getOptionValue("stop");
            String outputPath = cmd.getOptionValue("output");
            
        	
        	IndexWrapper index = IndexWrapperFactory.getIndexWrapper(indexPath);
        	Stopper stopper = new Stopper(stopPath);
            Map<Integer, FeatureVector> queries = readEvents(eventsPath, stopper);
            
        	// Setup the filter
	    	ChunkToFile f = new ChunkToFile();
	    	if (inputPath != null) {
	    		File infile = new File(inputPath);
	    		if (infile.isDirectory()) {
	                Iterator<File> files = FileUtils.iterateFiles(infile, null, true);

	                while (files.hasNext()) {
	                    File file = files.next();
						System.err.println(file.getAbsolutePath());
						f.filter(file, queries, index, stopper, outputPath);
					}
	    		}
	    		else
	    			System.err.println(infile.getAbsolutePath());
	    			f.filter(infile, queries, index, stopper, outputPath);
	    	}
	    
    	} catch (Exception e) {
    		e.printStackTrace();
    	}
	}

	public static Options createOptions()
	{
		Options options = new Options();
		options.addOption("input", true, "Input chunk file");
        options.addOption("events", true, "Events file");
        options.addOption("index", true, "Background statistics");
        options.addOption("stop", true, "Stoplist");
        options.addOption("output", true, "Output path");
		return options;
	}
	
	/**
	 * @param thriftFile
	 */
	public void filter(File infile, Map<Integer, FeatureVector> queries, IndexWrapper index, Stopper stopper, String outputPath) 
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
	        Pairtree ptree = new Pairtree();
	        
            try 
	        {
                // Run through items in the chunk file
	            while (true) 
	            {
	                final StreamItem item = new StreamItem();
	                item.read(inProtocol);

	                FeatureVector dv = new FeatureVector(item.body.clean_visible, stopper);
	                double maxScore = Double.NEGATIVE_INFINITY;
	                int qid = -1;
	                for (int id : queries.keySet()) 
	                {
	                    FeatureVector qv = queries.get(id);
	                    double score = kl(dv, qv, index, MU);
	                    if (score > maxScore) {
	                        qid = id;
	                        maxScore = score;
	                    }
	                }     
	                

	                String streamId = item.stream_id;
	                System.out.println(streamId + "=" + qid);
	                //System.out.println(streamId);
	                String ppath = ptree.mapToPPath(streamId.replace("-", ""));
	                //System.out.println(streamId + "=>" + ppath);
	                
	                File dir = new File(outputPath + File.separator + qid + File.separator + ppath);
	                dir.mkdirs();
	                XZOutputStream xos = new XZOutputStream(
	                        new FileOutputStream(dir.getAbsolutePath() + File.separator + streamId + ".xz"), 
	                        new LZMA2Options());
	                
	                TTransport outTransport = 
	                        new TIOStreamTransport(xos);
	                TBinaryProtocol outProtocol = new TBinaryProtocol(outTransport);
	                outTransport.open();
	                item.write(outProtocol);
	                outTransport.close();
	                
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
	
    public double kl (FeatureVector qv, FeatureVector dv, IndexWrapper index, double mu) {
        double ll = 0;
        
        for (String q: qv.getFeatures()) {
            double df = dv.getFeatureWeight(q);
            double dl = dv.getLength();
            double cp = ((1+ index.termFreq(q))/index.termCount());
            double pr = (df + mu*cp) / (dl + mu);
            ll += qv.getFeatureWeight(q) * Math.log(pr);
        }
        return ll;
    }
    
    public static Map<Integer, FeatureVector> readEvents(String path, Stopper stopper) 
    {
        Map<Integer, FeatureVector> queries = new TreeMap<Integer, FeatureVector>();

        try {
            DocumentBuilderFactory dbf = DocumentBuilderFactory
                    .newInstance();
            DocumentBuilder db = dbf.newDocumentBuilder();
            

            Document doc = db.parse(new File(path));
            
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
                queries.put(id, new FeatureVector(query, stopper));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return queries;
    }
}
