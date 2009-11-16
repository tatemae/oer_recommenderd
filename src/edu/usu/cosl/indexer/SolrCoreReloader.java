package edu.usu.cosl.indexer;

import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Enumeration;
import java.util.Vector;

import org.apache.solr.core.SolrCore;

public class SolrCoreReloader extends edu.usu.cosl.util.Daemon {
	private static void askSolrToReloadCore(String sCore) throws Exception {
		final String sReloadCommand = "http://localhost:8982/solr/admin/cores?action=RELOAD&core=";
		HttpURLConnection uriConn = (HttpURLConnection)new URL(sReloadCommand + sCore).openConnection();
    	InputStreamReader in = new InputStreamReader(uriConn.getInputStream(), "UTF8");
    	StringBuffer sb = new StringBuffer();
		char[] data = new char[1024];
        int len = in.read(data);
        if (len > 0) {
			while(len >= 0) {
				len = in.read(data);
				sb.append(data);
			}
        }
		in.close();
	}
	public static void askSolrToReloadIndexes(Vector<String> vNames) throws Exception{
		logger.debug("Telling solr to reload cores.");
		try {
			for (Enumeration<String> eCores = vNames.elements(); eCores.hasMoreElements();)
			{
				askSolrToReloadCore(eCores.nextElement());
			}
			logger.debug("Solr has reloaded its cores.");
		}catch(Exception e) {
			logger.debug("It looks like solr isn't running.");
		}
	}
}
