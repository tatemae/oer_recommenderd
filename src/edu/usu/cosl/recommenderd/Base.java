package edu.usu.cosl.recommenderd;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Properties;
import java.util.Vector;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.FSDirectory;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreContainer.Initializer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;

import edu.usu.cosl.util.DBThread;
import edu.usu.cosl.util.Logger;
import edu.usu.cosl.util.SendMail;

public class Base extends DBThread 
{
	protected Connection cn;

	protected Vector<SolrCore> vCores;
	protected CoreContainer mcore;
	protected Hashtable<String, Analyzer> htAnalyzers;
	protected Hashtable<String, IndexReader> htReaders;
	protected Hashtable<String, IndexSearcher> htSearchers;

	protected int nMaxRecommendations = 20;
	protected int nTagCloudDepth = 3;
	static protected String sSolrConfigFilename;

	private void configSolrLogging()
	{
		java.util.logging.Level level = java.util.logging.Level.OFF;
        java.util.logging.Logger.getLogger(SolrCore.class.getName()).setLevel(level);
        java.util.logging.Logger.getLogger(SolrResourceLoader.class.getName()).setLevel(level);
        java.util.logging.Logger.getLogger(org.apache.solr.schema.IndexSchema.class.getName()).setLevel(level);
        java.util.logging.Logger.getLogger(org.apache.solr.util.plugin.AbstractPluginLoader.class.getName()).setLevel(level);
        java.util.logging.Logger.getLogger(org.apache.solr.analysis.SynonymFilterFactory.class.getName()).setLevel(level);
        java.util.logging.Logger.getLogger(org.apache.solr.analysis.StopFilterFactory.class.getName()).setLevel(level);
        java.util.logging.Logger.getLogger(org.apache.solr.core.SolrConfig.class.getName()).setLevel(level);
        java.util.logging.Logger.getLogger(org.apache.solr.core.SolrConfig.HttpCachingConfig.class.getName()).setLevel(level);
        java.util.logging.Logger.getLogger(org.apache.solr.request.XSLTResponseWriter.class.getName()).setLevel(level);
        java.util.logging.Logger.getLogger(org.apache.solr.search.SolrIndexSearcher.class.getName()).setLevel(level);
        java.util.logging.Logger.getLogger(org.apache.solr.handler.component.SearchHandler.class.getName()).setLevel(level);
        java.util.logging.Logger.getLogger(org.apache.solr.core.CoreContainer.class.getName()).setLevel(level);
        java.util.logging.Logger.getLogger(org.apache.solr.update.DirectUpdateHandler2.class.getName()).setLevel(level);
        java.util.logging.Logger.getLogger(org.apache.solr.core.Config.class.getName()).setLevel(level);
        java.util.logging.Logger.getLogger(org.apache.solr.update.UpdateHandler.class.getName()).setLevel(level);
	}
	
	protected boolean bHarvest = true;
	protected boolean bRedoAllRecommendations = false;
	protected boolean bReIndexAll = false;
	
	protected void loadOptions(String sPropertiesFile)
	{
	    try 
	    {
	    	Properties properties = loadPropertyFile(sPropertiesFile);
	        
			// get some options out of it
	        String sValue = properties.getProperty("max_recommendations");
	        if (sValue != null) nMaxRecommendations = Integer.parseInt(sValue);
	        sValue = properties.getProperty("smtp_server");
	        if (sValue != null) {
	        	sSMTPServer = sValue;
	        	Logger.setLogToString(true);
	        }
	        sValue = properties.getProperty("admin_email");
	        if (sValue != null) sAdminEmail = sValue;
	        sValue = properties.getProperty("harvest");
	        if (sValue != null) bHarvest = "true".equals(sValue);
	        sValue = properties.getProperty("redo_recommendations");
	        if (sValue != null) bRedoAllRecommendations = "true".equals(sValue);
	        sValue = properties.getProperty("reindex_all");
	        if (sValue != null) bReIndexAll = "true".equals(sValue);
	        sValue = properties.getProperty("solr_config_filename");
	        if (sValue != null && System.getProperty("solr.solr.home") == null) sSolrConfigFilename = sValue;
	        sValue = properties.getProperty("tag_cloud_depth");
	        if (sValue != null) try{nTagCloudDepth = Integer.parseInt(sValue);}catch(Exception nfe){Logger.error("Unable to read tag_cloud_depth option value",nfe);} 
	        
	        getLoggerAndDBOptions(properties);
	    }
	    catch(Exception e){Logger.error(e);}
	    
	    configSolrLogging();
	}
	
	private String sSMTPServer = null;
	private String sAdminEmail = null;
	final static String sEmailFrom = "oerrecomender@cosl.usu.edu";
	final static String sReportSubject = "OER Recommender Harvest Report";
	public void notifyAdminOfResults()
	{
		if (sSMTPServer != null) 
			SendMail.sendMsg(sSMTPServer, sEmailFrom, sAdminEmail, sReportSubject, Logger.getMessages());
	}

	static protected String quoteEncode(String sText)
	{
		return sText == null ? null : sText.replaceAll("\"", "\\\\\"");
	}
	
	static protected String normalizedTitle(String sTitle)
	{
		if (sTitle == null) return "";
		int nIndex = sTitle.lastIndexOf(",");
		return nIndex == -1 ?  sTitle : sTitle.substring(0,nIndex);
	}
	
//	private int getNextID(String sTable) throws SQLException
//	{
//		ResultSet rsNextID = stNextID.executeQuery("SELECT nextval('" + sTable + "_id_seq')");
//		try
//		{
//			if (!rsNextID.next())
//			{
//				rsNextID.close();
//				throw new SQLException("Unable to retrieve the id for a newly added entry.");
//			}
//			int nNextID = rsNextID.getInt(1);
//			rsNextID.close();
//			return nNextID;
//		}
//		catch (SQLException e)
//		{
//			if (rsNextID != null) rsNextID.close();
//			throw e;
//		}
//	}
	
	protected int getLastID(Statement st) throws SQLException
	{
		ResultSet rsLastID = st.executeQuery("SELECT LAST_INSERT_ID()");
		try
		{
			if (!rsLastID.next())
			{
				rsLastID.close();
				throw new SQLException("Unable to retrieve the id for a newly added entry.");
			}
			int nLastID = rsLastID.getInt(1);
			if (nLastID == 0)
			{
				rsLastID.close();
				throw new SQLException("Unable to retrieve the id for a newly added entry.");
			}
			return nLastID;
		}
		catch (SQLException e)
		{
			if (rsLastID != null) rsLastID.close();
			throw e;
		}
	}
	
	protected Vector<Integer> getIDsOfEntries(String sCondition) throws SQLException
	{
		Statement stEntriesToIndex = cn.createStatement();
		ResultSet rsEntriesToIndex = stEntriesToIndex.executeQuery("SELECT id FROM entries " + sCondition);
		Vector<Integer> vIDs = new Vector<Integer>();
		while (rsEntriesToIndex.next())
		{
			vIDs.add(new Integer(rsEntriesToIndex.getInt(1)));
		}
		rsEntriesToIndex.close();
		stEntriesToIndex.close();
//		Logger.info("Entries to process: " + vIDs.size());
		return vIDs;
	}

	private void createAnalyzer(SolrCore core)
	{
		vCores.add(core);
		htAnalyzers.put(core.getName(), core.getSchema().getAnalyzer());
	}
	
	protected void createAnalyzers() throws Exception
	{
		if (mcore == null) initMultiCore();
		
		vCores = new Vector<SolrCore>();
		htAnalyzers = new Hashtable<String, org.apache.lucene.analysis.Analyzer>();

		for (Iterator<SolrCore> eCores = mcore.getCores().iterator(); eCores.hasNext();)
		{
			createAnalyzer(eCores.next());
		}
	}
	
	protected void initMultiCore() throws Exception 
	{
		Initializer initializer = new Initializer();
		if (sSolrConfigFilename != null) initializer.setSolrConfigFilename(sSolrConfigFilename);
		mcore = initializer.initialize();
	}

	protected void closeCores()
	{
		for (Enumeration<SolrCore> eCores = vCores.elements(); eCores.hasMoreElements();)
		{
			eCores.nextElement().close();
		}
		vCores.removeAllElements();
		vCores = null;
	}

	protected void closeIndexReaders() throws Exception
	{
		for (Enumeration<IndexReader> eReaders = htReaders.elements(); eReaders.hasMoreElements();)
		{
			eReaders.nextElement().close();
		}
		htReaders.clear();
		htReaders = null;
	}
	
	protected void createIndexReaders() throws Exception
	{
		htReaders = new Hashtable<String, IndexReader>();
		for (Enumeration<SolrCore> eCores = vCores.elements(); eCores.hasMoreElements();)
		{
			SolrCore core = eCores.nextElement();
			htReaders.put(core.getName(), IndexReader.open(FSDirectory.getDirectory(core.getIndexDir())));
		}
	}
	
	protected void createIndexSearchers() throws Exception
	{
		htSearchers = new Hashtable<String, IndexSearcher>();
		for (Enumeration<SolrCore> eCores = vCores.elements(); eCores.hasMoreElements();)
		{
			SolrCore core = eCores.nextElement();
			htSearchers.put(core.getName(), new IndexSearcher(htReaders.get(core.getName())));
		}
	}
	
	protected void closeIndexSearchers() throws Exception
	{
		for (Enumeration<IndexSearcher> eSearchers = htSearchers.elements(); eSearchers.hasMoreElements();)
		{
			eSearchers.nextElement().close();
		}
		htSearchers.clear();
		htSearchers = null;
	}
	
}
