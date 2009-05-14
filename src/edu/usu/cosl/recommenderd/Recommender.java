package edu.usu.cosl.recommenderd;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;

import java.util.Enumeration;
import java.util.Properties;
import java.util.Vector;
import java.util.Hashtable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Collections;

import java.io.File;
import java.io.FileInputStream;
import java.lang.Math;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.index.TermFreqVector;
import org.apache.lucene.search.Hit;
import org.apache.lucene.search.HitIterator;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.similar.MoreLikeThis;

import edu.usu.cosl.aggregatord.Harvester;
import edu.usu.cosl.util.DBThread;
import edu.usu.cosl.util.Logger;
import edu.usu.cosl.util.SendMail;

import org.apache.solr.core.MultiCore;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;

public class Recommender extends DBThread
{
	private Connection cnRecommender;
	private Statement stNextID;
	private PreparedStatement pstFlagEntryIndexed;
	private PreparedStatement pstFlagEntryRecommended;
    private PreparedStatement pstGetRecommendationID;
	private PreparedStatement pstAddRecommendation;
	private PreparedStatement pstUpdateRecommendation;
	private PreparedStatement pstEntriesPointingAtEntry;
	private PreparedStatement pstSetDocumentRecommendations;
	private PreparedStatement pstDeleteEntryRecommendations;
	private PreparedStatement pstDeleteEntry;
	private PreparedStatement pstAddTags;
	private PreparedStatement pstAddStemFreq;
	private PreparedStatement pstUpdateTags;
	private PreparedStatement pstGetTagName;
	private PreparedStatement pstAddTagsEntries;
	//private PreparedStatement sqlGetEntriesForTags;
	private PreparedStatement pstFlagTagsEntries;
	private PreparedStatement pstGenerateTags;
	private PreparedStatement pstAddPersonalRec;
	
	private String sSolrDir = "solr/";
	private MultiCore mcore;
	private Vector<SolrCore> vCores;
	private Hashtable<String, Analyzer> htAnalyzers;
	private Hashtable<String, IndexWriter> htWriters;
	private Hashtable<String, IndexReader> htReaders;
	private Hashtable<String, IndexSearcher> htSearchers;
	private int nMaxRecommendations = 20;
	private String sSMTPServer = null;
	private String sAdminEmail = null;
	private Logger log = new Logger();

	static private String quoteEncode(String sText)
	{
		return sText == null ? null : sText.replaceAll("\"", "\\\\\"");
	}
	
	static private String getEntryJSON(EntryInfo entry)
	{
		return "{" 
		+ "\"id\": " + entry.nRecommendationID 
		+ ", \"uri\": \"" + entry.sURI + "\"" 
		+ ", \"direct_link\": \"" + entry.sDirectLink + "\"" 
		+ ", \"title\": \"" + quoteEncode(entry.sTitle) + "\"" 
		+ ", \"collection\": \"" + quoteEncode(entry.sFeedShortTitle) + "\"" 
//		+ ", \"clicks\": 0" 
//		+ ", \"avg_time_on_target\": 60" 
		+ ", \"clicks\": " + entry.nClicks 
		+ ", \"avg_time_on_target\": " + entry.lAvgTimeAtDest 
		+ ", \"relevance\": " + entry.dRelevance 
		+ "}";
	}
	
	private double relevanceAverage(Vector<EntryInfo> vEntries)
	{
		double dSum = 0;
		for (Enumeration<EntryInfo> eEntries = vEntries.elements(); eEntries.hasMoreElements();)
		{
			EntryInfo entry = eEntries.nextElement();
			dSum += entry.dRelevance;
		}
		return dSum / vEntries.size();
	}
	private double relevanceStandardDeviation(double dAverage, Vector<EntryInfo> vEntries)
	{
		double dSum = 0;
		for (Enumeration<EntryInfo> eEntries = vEntries.elements(); eEntries.hasMoreElements();)
		{
			EntryInfo entry = eEntries.nextElement();
			dSum += Math.pow((entry.dRelevance - dAverage),2);
		}
		return Math.sqrt(dSum / vEntries.size());
	}
	
	private double calcRelevanceThreshold(Vector<EntryInfo> vEntries)
	{
		if (vEntries.size() == 0) return 0;
		double dAverage = relevanceAverage(vEntries);
		double dStandardDeviation = relevanceStandardDeviation(dAverage, vEntries);
		return dAverage + dStandardDeviation;
	}
	
	private double clickAverage(Vector<EntryInfo> vEntries)
	{
		double dSum = 0;
		for (Enumeration<EntryInfo> eEntries = vEntries.elements(); eEntries.hasMoreElements();)
		{
			EntryInfo entry = eEntries.nextElement();
			dSum += entry.nClicks;
		}
		return dSum / vEntries.size();
	}
	private double clickStandardDeviation(double dAverage, Vector<EntryInfo> vEntries)
	{
		double dSum = 0;
		for (Enumeration<EntryInfo> eEntries = vEntries.elements(); eEntries.hasMoreElements();)
		{
			EntryInfo entry = eEntries.nextElement();
			dSum += Math.pow((entry.nClicks - dAverage),2);
		}
		return Math.sqrt(dSum / vEntries.size());
	}
	
	private int calcClickThreshold(Vector<EntryInfo> vEntries)
	{
		int nClickThreshold = 5;
		if (vEntries.size() != 0)
		{
			double dAverage = clickAverage(vEntries);
			double dStandardDeviation = clickStandardDeviation(dAverage, vEntries);
			nClickThreshold = (int)Math.round(dAverage + dStandardDeviation);
		}
		return nClickThreshold > 5 ? nClickThreshold : 5;
	}
	
	// we calculate popularity by combining average time on page and number of clicks
	// average time on page is the true measure of popularity
	// number of clicks adds certainty to the average time on page
	// first we find the entries that have been clicked on a lot more than other pages
	// then for those entries, we rank them according to average time on page
	
	private void storeRecommendationsInEntry(EntryInfo entry, Vector<EntryInfo> vRelatedEntries) throws SQLException
	{
		double dThreshold = calcRelevanceThreshold(vRelatedEntries);
		int nClickThreshold = calcClickThreshold(vRelatedEntries);
		
		String sPopular = "[";
		String sRelevant = "[";
		String sOther = "[";
		
		Vector<EntryInfo> vPopular = new Vector<EntryInfo>(); 
		
		for (Enumeration<EntryInfo> eRelatedEntries = vRelatedEntries.elements(); eRelatedEntries.hasMoreElements();)
		{
			EntryInfo relatedEntry = eRelatedEntries.nextElement();
			if (relatedEntry.nClicks >= nClickThreshold)
			{
				vPopular.add(relatedEntry);
				continue;
			}
			String sJSON = getEntryJSON(relatedEntry);
			if (relatedEntry.dRelevance > dThreshold) sRelevant += sRelevant.length() > 1 ? "," + sJSON : sJSON; 
			else sOther += sOther.length() > 1 ? "," + sJSON : sJSON;
		}
		
		// sort the popular entries by average time on page
		Collections.sort(vPopular);
		for (Enumeration<EntryInfo> ePopular = vPopular.elements(); ePopular.hasMoreElements();)
		{
			EntryInfo popularEntry = ePopular.nextElement();
			String sJSON = getEntryJSON(popularEntry);
			sPopular += sPopular.length() > 1 ? "," + sJSON : sJSON; 
		}
		
		sPopular += "]";
		sRelevant += "]";
		sOther += "]";
		
//		Logger.info("Relevant: " + sRelevant);
//		Logger.info("Other: " + sOther);
		
		pstSetDocumentRecommendations.setString(1, sPopular);
		pstSetDocumentRecommendations.setString(2, sRelevant);
		pstSetDocumentRecommendations.setString(3, sOther);
		pstSetDocumentRecommendations.setInt(4, entry.nEntryID);
		pstSetDocumentRecommendations.executeUpdate();
	}
	
	private String getCommaSeparatedList(Vector<EntryInfo> vEntries)
	{
		Enumeration<EntryInfo> eEntries = vEntries.elements();
		String sList = "(" + eEntries.nextElement().nEntryID;
		while (eEntries.hasMoreElements())
		{
			sList += ", " + eEntries.nextElement().nEntryID;
		}
		return sList + ")";
	}
	
	private void deleteNoLongerRelevantRecommendations(EntryInfo entry, Vector<EntryInfo> vRecommendations) throws SQLException
	{
		Statement stDeleteRecommendations = cnRecommender.createStatement();
		String sIDList = getCommaSeparatedList(vRecommendations);
		stDeleteRecommendations.executeUpdate("DELETE FROM recommendations WHERE entry_id = " + entry.nEntryID + " AND dest_entry_id NOT IN " + sIDList);
		stDeleteRecommendations.close();
	}
	
	private void flagEntryRecommended(int nEntryID) throws SQLException
	{
		pstFlagEntryRecommended.setInt(1, nEntryID);
		pstFlagEntryRecommended.addBatch();
	}
	
	private void updateRecommendationsForEntry(EntryInfo entry) throws Exception
	{
		try
		{
			Vector<EntryInfo> vRecommendations = getRelatedEntries(entry);
			if (vRecommendations.size() > 0)
			{
				addRecommendationsToDB(entry, vRecommendations);
				storeRecommendationsInEntry(entry,vRecommendations);
				deleteNoLongerRelevantRecommendations(entry, vRecommendations);
			}
			else flagEntryRecommended(entry.nEntryID);
		}
		catch (Exception e)
		{
			Logger.error("Error in updateEntryRecommendations");
			throw e;
		}
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
	
	private void addRecommendation(EntryInfo entry, EntryInfo relatedEntry, int nRank) throws SQLException
	{
//		relatedEntry.nRecommendationID = getNextID("recommendations");
//		pstAddRecommendation.setInt(1, relatedEntry.nRecommendationID);
		pstAddRecommendation.setInt(1, entry.nEntryID);
		pstAddRecommendation.setInt(2, relatedEntry.nEntryID);
		pstAddRecommendation.setInt(3, nRank);
		pstAddRecommendation.setDouble(4, relatedEntry.dRelevance);
		pstAddRecommendation.addBatch();
	}
	
	private void updateRecommendation(EntryInfo relatedEntry, int nRank) throws SQLException
	{
		pstUpdateRecommendation.setInt(1, nRank);
		pstUpdateRecommendation.setDouble(2, relatedEntry.dRelevance);
		pstUpdateRecommendation.setInt(3, relatedEntry.nRecommendationID);
		pstUpdateRecommendation.addBatch();
	}
	
	private void getRecommendationInfo(int nEntryID, EntryInfo relatedEntry) throws SQLException
	{
		pstGetRecommendationID.setInt(1, nEntryID);
		pstGetRecommendationID.setInt(2, relatedEntry.nEntryID);
		ResultSet rsRecommendationID = pstGetRecommendationID.executeQuery();
		if (rsRecommendationID.next())
		{
			relatedEntry.nRecommendationID = rsRecommendationID.getInt("id");
			relatedEntry.nClicks = rsRecommendationID.getInt("clicks");
			relatedEntry.lAvgTimeAtDest = rsRecommendationID.getLong("avg_time_at_dest");
		}
		else relatedEntry.nRecommendationID = 0;
		rsRecommendationID.close();
	}

	private void addRecommendationsToDB(EntryInfo entry, Vector<EntryInfo> vRelatedEntries) throws SQLException
	{
		try
		{
			int nRank = 1;
			for (Enumeration<EntryInfo> eEntries = vRelatedEntries.elements(); eEntries.hasMoreElements();)
			{
				EntryInfo relatedEntry = eEntries.nextElement();
	
				getRecommendationInfo(entry.nEntryID, relatedEntry);
				
				if (relatedEntry.nRecommendationID == 0)
				{
					addRecommendation(entry, relatedEntry, nRank);
				}
				else
				{
					updateRecommendation(relatedEntry, nRank);
				}
				nRank++;
			}
			pstAddRecommendation.executeBatch();
		}
		catch (SQLException e)
		{
			Logger.error("Error in addRecommendationsToDB");
			throw e;
		}
	}
	
//	Hashtable<Integer, String> htShortFeedTitles = new Hashtable<Integer, String>();
//	private void loadShortFeedTitles()
//	{
//		
//	}
	
	private String normalizedTitle(String sTitle)
	{
		if (sTitle == null) return "";
		int nIndex = sTitle.lastIndexOf(",");
		return nIndex == -1 ?  sTitle : sTitle.substring(0,nIndex);
	}
	
	private Vector<EntryInfo> getRelatedEntries(EntryInfo entry) throws Exception
	{
		Vector<EntryInfo> vEntries = new Vector<EntryInfo>();
		try
		{
			// if we don't have an anlyzer for the language, bail
			Analyzer analyzer = htAnalyzers.get(entry.sLanguage);
		    if (analyzer == null) return vEntries;
		    
		    // find the lucene document for the entry
			TermQuery query = new TermQuery(new Term("id","Entry:" + entry.nEntryID));
			IndexSearcher searcher = htSearchers.get(entry.sLanguage);
		    Hits hits = searcher.search(query);
		    int nDocID = hits.id(0);
		    if (hits.length() == 0 || nDocID == 0) return vEntries;
		    
		    // ask lucene for more entries like this on
		    IndexReader reader = htReaders.get(entry.sLanguage);
		    MoreLikeThis mlt = new MoreLikeThis(reader);
		    mlt.setMinTermFreq(1);
		    mlt.setAnalyzer(analyzer);
		    mlt.setFieldNames(new String[]{"text"});
		    mlt.setMinWordLen(("zh".equals(entry.sLanguage) || "ja".equals(entry.sLanguage)) ? 1 : 4);
		    mlt.setMinDocFreq(2);
		    mlt.setBoost(true);
		    Query like = mlt.like(nDocID);
		    Logger.info("Query terms: " + like.toString().split("text:").length);
		    Logger.info(like.toString());
//		    Logger.info("title: " + entry.sTitle);
//		    Logger.info("description: " + entry.sDescription);
//		    Logger.info("tag_list: " + entry.sTagList);
//		    Logger.info(like.toString());
		    Hits relatedDocs = searcher.search(like);
		    
			int nSameDomainHits = 0;
			int nOtherDomainHits = 0;
			int nEnd = entry.sURI.indexOf("/", 10);
			String sDomain = nEnd == -1 ? entry.sURI : entry.sURI.substring(0, nEnd);
			String sNormalizedEntryTitle = normalizedTitle(entry.sTitle);
			HashSet<String> hsRecommendations = new HashSet<String>();
			hsRecommendations.add(entry.sURI);
			if (entry.sDirectLink != null) hsRecommendations.add(entry.sDirectLink);
			int nHit = 0;
		    for (HitIterator docs = (HitIterator)relatedDocs.iterator(); docs.hasNext() && !(nSameDomainHits == nMaxRecommendations && nOtherDomainHits == nMaxRecommendations) && nHit < 100;)
		    {
				nHit++;
	
				Hit hit = (Hit)docs.next();
		    	Document lDoc = hit.getDocument();
		    	
		    	// don't include ourselves as a recommendation
		    	if (nDocID == hit.getId()) continue;
		    	int id=hit.getId();

		    	String sTitle = null;
				String sNormalizedTitle = null;
				String sURI = null;
				String sDirectLink = null;
				double dRelevance = 0;
		    	try
		    	{
			    	sTitle = lDoc.getField("title").stringValue();
					sNormalizedTitle = normalizedTitle(sTitle);
					sURI = lDoc.getField("permalink").stringValue();
					Field directLink = lDoc.getField("direct_link");
					sDirectLink = directLink == null ? "" : directLink.stringValue();
					dRelevance = hit.getScore();
		    	}
		    	catch (Exception e3)
		    	{
		    		// some times for some strange reason entries don't have permalinks and this blows chunks
		    		continue;
		    	}
	
		    	// avoid adding duplicate or near duplicate titles (MIT courses offered at different times)
				if (!(
						hsRecommendations.contains(sNormalizedTitle) // same title 
					 || hsRecommendations.contains(sURI) // same uri
					 || hsRecommendations.contains(sDirectLink) // same direct link
					 || (dRelevance > 0.99 && sNormalizedTitle.equals(sNormalizedEntryTitle)) // perfect relevance and same title as the source document
				   ))
				{
//					Logger.info("hit: " + nHit++);
					boolean bSameDomain = sURI.startsWith(sDomain);
					
					if (nSameDomainHits == nMaxRecommendations && bSameDomain ||
						nOtherDomainHits == nMaxRecommendations && !bSameDomain) continue;
					
					if (bSameDomain) nSameDomainHits++;
					else nOtherDomainHits++;
					
			    	EntryInfo relatedEntry = new EntryInfo();
			    	relatedEntry.nEntryID = Integer.parseInt(lDoc.getField("pk_i").stringValue());
			    	relatedEntry.sURI = sURI;
			    	relatedEntry.sDirectLink = sDirectLink;
			    	relatedEntry.sTitle = sTitle;
			    	relatedEntry.dRelevance = dRelevance;
			    	Integer intFeedID = new Integer(lDoc.getField("feed_id_i").stringValue());
			    	relatedEntry.nFeedID = intFeedID.intValue();
			    	relatedEntry.sFeedShortTitle = lDoc.getField("collection").stringValue(); 
			    	
//			    	Logger.info("Relevance:" + relatedEntry.dRelevance + " " + nSameDomainHits + ", " + nOtherDomainHits);

			    	vEntries.add(relatedEntry);

			    	hsRecommendations.add(sNormalizedTitle);
					hsRecommendations.add(sURI);
					if (sDirectLink != null) hsRecommendations.add(sDirectLink);
				}
			}
		}
		catch (Exception e)
		{
			Logger.error("Error processing getRelatedEntries for entry ID: " + entry.nEntryID);
			Logger.error(e);
		}
		return vEntries;
	}
	
	private void updateRecommendations(boolean bAll) throws SQLException
	{
		Vector<Integer> vIDs = getIDsOfEntries(bAll ? "WHERE substring(language,1,2) IN ('en', 'es', 'zh', 'fr', 'ja', 'de', 'ru', 'nl')" : "WHERE indexed_at > relevance_calculated_at AND substring(language,1,2) IN ('en', 'es', 'zh', 'fr', 'ja', 'de', 'ru', 'nl')");
		Logger.status("updateRecommendations - begin (entries to update): " + vIDs.size());
		updateRecommendations(vIDs);
		Logger.status("updateRecommendations - end");
	}
	private void updatePersonalRecommendations(boolean bAll) throws SQLException
	{
		Statement stTruncPersonalRec=cnRecommender.createStatement();
		stTruncPersonalRec.executeQuery("TRUNCATE TABLE personal_recommendations");
		//Join table attentions, action_type, recommendations to generate table personal_recommendations
		pstAddPersonalRec=cnRecommender.prepareStatement(
				"INSERT INTO personal_recommendations (personal_recommendable_id,personal_recommendable_type,destination_id,relevance)" +
				"SELECT at.attentionable_id, at.attentionable_type,r.dest_entry_id,"+
				"MAX((at.weight)*(ac.weight)*(r.relevance)) as score "+
				"FROM attentions at, action_types ac, recommendations r "+
				"WHERE at.action_type=ac.action_type AND at.entry_id=r.entry_id "+
				"GROUP BY dest_entry_id , attentionable_id "+
				"ORDER BY attentionable_id");
		pstAddPersonalRec.execute();
		pstAddPersonalRec.close();
		
		
	}
	private void updateRecommendations(Vector<Integer> vIDs)
	{
		if (vIDs.size() == 0) return;

		cleanupIndex();
		
		try
		{
			createIndexReaders();
			createIndexSearchers();
		    
		    try
		    {
				int nEntry = 0;
	
				stNextID = cnRecommender.createStatement();
				
				pstGetRecommendationID = cnRecommender.prepareStatement(
					"SELECT id, clicks, avg_time_at_dest FROM recommendations WHERE entry_id = ? AND dest_entry_id = ?");
				
				pstAddRecommendation = cnRecommender.prepareStatement(
					"INSERT INTO recommendations (entry_id, dest_entry_id, rank, relevance) " +
					"VALUES (?, ?, ?, ?)");
				
				pstUpdateRecommendation = cnRecommender.prepareStatement(
					"UPDATE recommendations SET rank = ?, relevance = ? WHERE id = ? ");
				
				pstSetDocumentRecommendations = cnRecommender.prepareStatement(
					"UPDATE entries SET popular = ?, relevant = ?, other = ?, " + 
					"relevance_calculated_at = now() WHERE id = ?");
				
				pstFlagEntryRecommended = cnRecommender.prepareStatement(
					"UPDATE entries SET relevance_calculated_at = now() WHERE id = ?");
		
				PreparedStatement pstEntryToCreateRecommendationsFor = cnRecommender.prepareStatement(
					"SELECT id, feed_id, permalink, direct_link, title, description, tag_list, language " +
					"FROM entries WHERE id = ?");
				
				for (Enumeration<Integer> eIDs = vIDs.elements(); eIDs.hasMoreElements();)
				{
					pstEntryToCreateRecommendationsFor.setInt(1, eIDs.nextElement().intValue());
					ResultSet rsEntryToCreateRecommendationsFor = pstEntryToCreateRecommendationsFor.executeQuery();
					if (rsEntryToCreateRecommendationsFor.next())
					{
						updateRecommendationsForEntry(new EntryInfo(rsEntryToCreateRecommendationsFor));
						nEntry++;
					}
					rsEntryToCreateRecommendationsFor.close();
					if (nEntry % 100 == 0)
					{
						pstSetDocumentRecommendations.executeBatch();
						pstUpdateRecommendation.executeBatch();
						pstAddRecommendation.executeBatch();
					}
					if (nEntry % 1000 == 0)Logger.status("Recommending: " + nEntry);
					else if (nEntry % 100 == 0)Logger.info("Recommending: " + nEntry);
				}
				pstEntryToCreateRecommendationsFor.close();
	
				pstFlagEntryRecommended.executeBatch();
				pstSetDocumentRecommendations.executeBatch();
				pstUpdateRecommendation.executeBatch();
				pstAddRecommendation.executeBatch();
				
				pstFlagEntryRecommended.close();
				pstSetDocumentRecommendations.close();
				pstUpdateRecommendation.close();
				pstAddRecommendation.close();
				
				pstGetRecommendationID.close();
				
				stNextID.close();
			}
		    catch (Exception e)
		    {
				Logger.error("updateRecommendations(1) - ", e);
		    }
		    closeIndexSearchers();
		    closeIndexReaders();
		}
		catch (Exception e)
		{
			Logger.error("updateRecommendations(2) - ", e);
		}
	}
	
	
	class TagStemmer extends org.apache.lucene.analysis.Tokenizer
    {
		private org.apache.solr.analysis.EnglishPorterFilterFactory factoryen;
		private edu.usu.cosl.analysis.es.SpanishPorterFilterFactory factoryes;
		private org.apache.solr.analysis.GermanStemFilterFactory factoryde;
		private org.apache.solr.analysis.FrenchStemFilterFactory factoryfr;
		private org.apache.solr.analysis.EnglishPorterFilterFactory factoryja;
		private org.apache.solr.analysis.DutchStemFilterFactory factorynl;
		private org.apache.solr.analysis.RussianStemFilterFactory factoryru;
		private org.apache.solr.analysis.ChineseFilterFactory factoryzh;
        private org.apache.lucene.analysis.Token token;
        private org.apache.lucene.analysis.TokenStream tokenStream;
        
        public TagStemmer(String language) 
        {
        	if("en".equals(language))
        	{
        		factoryen = new org.apache.solr.analysis.EnglishPorterFilterFactory();
        		tokenStream = factoryen.create(this);
        	}
        	else if("es".equals(language))
        	{
        		factoryes = new edu.usu.cosl.analysis.es.SpanishPorterFilterFactory();
        		tokenStream = factoryes.create(this);
        	}
        	else if("de".equals(language))
        	{
        		factoryde=new org.apache.solr.analysis.GermanStemFilterFactory();
        		tokenStream = factoryde.create(this);
        	}
        	else if("fr".equals(language))
        	{
        		factoryfr=new org.apache.solr.analysis.FrenchStemFilterFactory();
        		tokenStream = factoryfr.create(this);
        	}
        	else if("ja".equals(language))
        	{
        		factoryja = new org.apache.solr.analysis.EnglishPorterFilterFactory();
        		tokenStream = factoryja.create(this);
        	    	
        	}
        	else if("nl".equals(language))
        	{
        		factorynl = new org.apache.solr.analysis.DutchStemFilterFactory();
        		tokenStream = factorynl.create(this);
        	}
        	else if("ru".equals(language))
        	{
        		factoryru = new org.apache.solr.analysis.RussianStemFilterFactory();
        		tokenStream = factoryru.create(this);
        	}
        	else if("zh".equals(language))
        	{
        		tokenStream = new org.apache.solr.analysis.ChineseFilterFactory().create(this);
        	}
        	token = new org.apache.lucene.analysis.Token();
        	
        }
        public org.apache.lucene.analysis.Token next()
        {
            return token; 
        }
        @SuppressWarnings("deprecation")
		public String getStem(String sTerm)
        {
        	token.setTermText(sTerm);
            try {
            	return tokenStream.next().termText();
            } catch (java.io.IOException e) {
                Logger.error(e);
                return sTerm;
            }
        }
    }
	private void getNonStemTerms() {
		
		
		try {
			Object[] languages=htReaders.keySet().toArray();
			for(int i=0; i<languages.length;i++)
			{
				
			//	s=htReaders.keySet().iterator().next().;
				TagStemmer stemmer = new TagStemmer(languages[i].toString());
				IndexReader reader = htReaders.get(languages[i].toString());
				TermEnum te = reader.terms(new Term ("tag", ""));
				int numoftags=0;
				while (te.next()){
					Term term = te.term();
					int f = te.docFreq();
					
					if(f>0&&"tag".equals(te.term().field()))
					{
						numoftags++;
						String output=term.text().toString();
						pstGetTagName.setString(1, output);
						pstGetTagName.setInt(2, f);
						ResultSet res=pstGetTagName.executeQuery();
						if(res.next())
						{
							pstUpdateTags.setInt(1, f);
							pstUpdateTags.setString(2, output);
							pstUpdateTags.addBatch();
							
						}
						else
						{
							pstAddTags.setInt(1, 0);
							pstAddTags.setString(2, output);
							pstAddTags.setString(3, stemmer.getStem(output));
							pstAddTags.setInt(4, f);
							pstAddTags.addBatch();
							
						}
						
						if (numoftags%1000==0)
						{
							pstAddTags.executeBatch();
							pstUpdateTags.executeBatch();
							Logger.status("WriteTagstoDB: " + numoftags);
							
						}
						
						
						
					}
					
				}
				if (numoftags%1000!=0)
				{
					pstAddTags.executeBatch();
					pstUpdateTags.executeBatch();
					Logger.status("WriteTagstoDB: " + numoftags);
				}
			}		
		} catch (Exception e) {
			Logger.info(e.toString());
		}
	}
	private void getStemTerms() {
		
		
		try {
			//TagStemmer stemmer = new TagStemmer();
			Object[] languages = htReaders.keySet().toArray();
			for(int i=0; i<languages .length;i++)
			{
				int numofstems=0;
				IndexReader reader = htReaders.get(languages[i].toString());
				
				TermEnum te = reader.terms(new Term ("text", ""));
				while (te.next()){
					Term term = te.term();
					int f = te.docFreq();
					if(f>0&&"text".equals(te.term().field()))
					{
						numofstems++;
						String output=term.text().toString();
						pstAddStemFreq.setInt(1, f);
						pstAddStemFreq.setString(2, output);
						pstAddStemFreq.addBatch();
						if ("a".equals(output.toString()))
						{
							Logger.status("get it");
						}
						if (numofstems%1000==0)
						{
							pstAddStemFreq.executeBatch();
							Logger.status("WriteStemstoDB: " + numofstems);
							
						}
						
					}
					
				}
				if(numofstems%1000!=0)
				{
					pstAddStemFreq.executeBatch();
					Logger.status("WriteStemstoDB: " + numofstems);
				}
			}
		
		} catch (Exception e) {
			Logger.info(e.toString());
		}
	}
	private void generateTagsEntries() {
	    try
	    {
	    	IndexReader reader = htReaders.get("en");
	    	int num=0;
	    	int numofdoc=reader.numDocs();
	    	//sqlAdd
	    	pstAddTagsEntries=cnRecommender.prepareStatement(
	    			"INSERT INTO entries_tags (entry_id, tag_id) "+
	    			"VALUES (?,?) ");
	    	pstFlagTagsEntries=cnRecommender.prepareStatement(
	    			"SELECT * FROM entries_tags where entry_id=? AND tag_id=?");
	    	
	    	while(num<numofdoc)
	    	{
	    		Statement sqlGetEntriesForTags=cnRecommender.createStatement();
	    		TermFreqVector termFreqVector = reader.getTermFreqVector(num, "text");
	    	    Document doc=reader.document(num);
	    	    int entryid=Integer.parseInt(doc.getField("id").stringValue().substring(6));
	    	    if (termFreqVector!=null)
	    	    {
	    	    	String[] terms=termFreqVector.getTerms();
	    	    	String termlist="\""+terms[0]+"\""; 
	    	    	for(int i=1;i<terms.length;i++)
	    	    	{
	    	    		termlist=termlist+","+"\""+terms[i]+"\"";
	    	    	}
	    	    	ResultSet rootIdindex=sqlGetEntriesForTags.executeQuery("SELECT id FROM tags WHERE root=1 AND stem in (" +
	    	    			termlist + ")");
	    	    	while(rootIdindex.next())
	    	    	{
	    	    		int rootid=rootIdindex.getInt(1);
	    	    		pstAddTagsEntries.setInt(1, entryid);
	    	    		pstAddTagsEntries.setInt(2, rootid);
	    	    		pstAddTagsEntries.addBatch();
	    	    		
	    	    	}
	    	    	
	    	    	/*for(int i=0;i<terms.length;i++)
	    	    	{
	    	    		sqlGetEntriesForTags.setString(1, terms[i]);
	    	    		ResultSet rootIdindex=sqlGetEntriesForTags.executeQuery();
	    	    		sqlGetEntriesForTags.clearParameters();
	    	    		int rootid;
	    	    		rootIdindex.next();
	    	    		rootid=rootIdindex.getInt(1);
	    	    		sqlFlagTagsEntries.setInt(1, entryid);
	    	    		sqlFlagTagsEntries.setInt(2, rootid);
	    	    		ResultSet flagofexists=sqlFlagTagsEntries.executeQuery();
	    	    		if(!flagofexists.next())
	    	    		{
		    	    		sqlAddTagsEntries.setInt(1, entryid);
		    	    		sqlAddTagsEntries.setInt(2, rootid);
		    	    		sqlAddTagsEntries.addBatch();
	    	    		}
	    	    	}*/
	    	    	pstAddTagsEntries.executeBatch();
	    	    	
	    	    }
	    	    sqlGetEntriesForTags.close();
	    		if(num%100==0)
	    		{
	    			Logger.status("WriteintoTagsEntries"+num);
	    		}
	    		num++;
	    	}
	    	if(num%100!=0)
	    	{
		    	//sqlAddTagsEntries.executeBatch();
				Logger.status("WriteintoTagsEntries"+num);
	    	}
	    	pstAddTagsEntries.close();
	    	pstFlagTagsEntries.close();
	    }
	    catch (Exception e)
	    {
	    	Logger.info(e.toString());
	    }
	}

	private void updateTags()
	{
		try
		{
			createIndexReaders();
			createIndexSearchers();
			pstGetTagName =cnRecommender.prepareStatement(
					"SELECT id FROM tags WHERE name=? and frequency=?");
			pstAddTags = cnRecommender.prepareStatement(
					"INSERT INTO tags (id, name, stem, frequency) " +
					"VALUES (?, ?, ?, ?)");
			pstUpdateTags = cnRecommender.prepareStatement(
					"UPDATE tags "+
					"SET frequency =? " +
					"where name = ? "
					);
			getNonStemTerms();
			pstAddStemFreq =cnRecommender.prepareStatement(
                    "UPDATE tags SET stem_frequency = ? where stem = ? ");
			getStemTerms();
			pstGenerateTags =cnRecommender.prepareStatement(
					"UPDATE tags,"+
					"(SELECT a.id, a.name,(SELECT max(frequency) "+
					"FROM tags WHERE a.stem=stem AND a.frequency>=frequency) AS frequency, a.stem,"+
					"NOT EXISTS (SELECT id FROM tags WHERE a.stem=stem AND a.frequency<frequency) AS root FROM tags a) c " +
					"SET tags.root=c.root WHERE tags.id=c.id ");
			pstGenerateTags.executeUpdate();
			Statement sqlMinLenName=cnRecommender.createStatement(); 
			sqlMinLenName.execute("UPDATE tags, (SELECT id FROM tags WHERE root=1 GROUP BY stem ORDER BY length(name))b SET tags.root=null WHERE tags.root=1 AND tags.id =b.id");
			Statement sqlZeroMinLenName=cnRecommender.createStatement(); 
			sqlZeroMinLenName.execute("UPDATE tags SET root=0 WHERE root=1");
			Statement sqlFlagMinLenName=cnRecommender.createStatement(); 
			sqlFlagMinLenName.execute("UPDATE tags SET root=1 WHERE root IS NULL");
			generateTagsEntries();
			pstGetTagName.close();
			pstAddTags.close();
			pstUpdateTags.close();
			pstAddStemFreq.close();
			pstGenerateTags.close();
			sqlMinLenName.close();
			sqlZeroMinLenName.close();
			sqlFlagMinLenName.close();
		    closeIndexSearchers();
		    closeIndexReaders();
		}
		catch (Exception e)
		{
			Logger.error("updateTags - ", e);
		}
	}
	private void updateQueries()
	{
		try
		{
			Statement stSubjectFreq=cnRecommender.createStatement();
			ResultSet subjectFreq=stSubjectFreq.executeQuery("select subject_id, count(*) from entries_subjects group by subject_id");
			subjectFreq.last();
			int num=subjectFreq.getRow();
			subjectFreq.first();
			int []count=new int[num];
			int i=0,max=0;
			do
		    {
		    	count[i]=subjectFreq.getInt(2);
		    	if(max<=count[i])
		    		max=count[i];
		    	i++;
		    }while(subjectFreq.next());
			
			PreparedStatement insertIntoQueries=cnRecommender.prepareStatement("Insert Into queries(name,frequency)"+
					"values ((select name from subjects where subjects.id=?),?)");
			PreparedStatement searchSubsinQueries=cnRecommender.prepareStatement("SELECT queries.id,queries.frequency FROM queries, subjects "+
			"where subjects.name=queries.name and subjects.id=?");
			PreparedStatement updateIntoQueries=cnRecommender.prepareStatement("Update queries set frequency=? where id=?");
			for(i=0;i<num;i++)
			{
				count[i]=(int)((float)(count[i])/(float)(max)*100.0);
				searchSubsinQueries.setInt(1, i+1);
				ResultSet subsinQueries =searchSubsinQueries.executeQuery();
				if(subsinQueries.next())
				{
					if(subsinQueries.getInt(2)<100)
					{
						updateIntoQueries.setInt(1, count[i]);
						updateIntoQueries.setInt(2, subsinQueries.getInt(1));
						updateIntoQueries.addBatch();
					}
				}
				else
				{
					insertIntoQueries.setInt(1, i+1);
					insertIntoQueries.setInt(2, count[i]);
					insertIntoQueries.addBatch();
				}
			}
			insertIntoQueries.executeBatch();
			updateIntoQueries.executeBatch();
		}
		catch (Exception e)
		{
			Logger.error("updateQueries - ", e);
		}
		
	}

	private void indexEntry(EntryInfo entry) throws Exception 
	{
		// don't put into lucene entries whose language we don't have an analyzer for
		Analyzer analyzer = htAnalyzers.get(entry.sLanguage);
		if (analyzer != null)
		{
			htWriters.get(entry.sLanguage).updateDocument(new Term("id","Entry:" + entry.nEntryID), LuceneDocument.Document(entry), analyzer);
		}
		pstFlagEntryIndexed.setInt(1, entry.nEntryID);
		pstFlagEntryIndexed.addBatch();
	}
	
	private Vector<Integer> getIDsOfEntries(String sCondition) throws SQLException
	{
		Statement stEntriesToIndex = cnRecommender.createStatement();
		ResultSet rsEntriesToIndex = stEntriesToIndex.executeQuery("SELECT id FROM entries " + sCondition);
		Vector<Integer> vIDs = new Vector<Integer>();
		while (rsEntriesToIndex.next())
		{
			vIDs.add(new Integer(rsEntriesToIndex.getInt(1)));
		}
		rsEntriesToIndex.close();
		stEntriesToIndex.close();
		Logger.info("Entries to process: " + vIDs.size());
		return vIDs;
	}

	private void removeEntryFromIndex(EntryInfo entry) throws Exception
	{
		// don't put into lucene entries whose language we don't have an analyzer for
		htWriters.get(entry.sLanguage).deleteDocuments(new Term("id","Entry:" + entry.nEntryID));
	}
	
	private void getIDsOfEntriesPointingAtEntry(Vector<Integer> vIDs, int nEntryID) throws SQLException
	{
		pstEntriesPointingAtEntry.setInt(1, nEntryID);
		ResultSet rsEntries = pstEntriesPointingAtEntry.executeQuery();
		while (rsEntries.next())
		{
			vIDs.add(new Integer(rsEntries.getInt(1)));
		}
		rsEntries.close();
	}
	
	private Vector<EntryInfo> getDeletedEntries() throws SQLException
	{
		Vector<EntryInfo> vEntries = new Vector<EntryInfo>();

		Statement stEntries = cnRecommender.createStatement();
		ResultSet rsEntries = stEntries.executeQuery("SELECT id, language FROM entries WHERE oai_identifier = 'deleted'");
		while (rsEntries.next())
		{
			EntryInfo entry = new EntryInfo();
			entry.nEntryID = rsEntries.getInt(1);
			entry.sLanguage = rsEntries.getString(2);
			vEntries.add(entry);
		}
		rsEntries.close();
		stEntries.close();
		return vEntries;
	}
	
	private void deleteRecommendationsInvolvingEntry(int nEntryID) throws Exception
	{
		// delete recommendations involving the entry being deleted 
		pstDeleteEntryRecommendations.setInt(1, nEntryID);
		pstDeleteEntryRecommendations.setInt(2, nEntryID);
		pstDeleteEntryRecommendations.executeUpdate();
		
		// delete the entry
		pstDeleteEntry.setInt(1, nEntryID);
		pstDeleteEntry.executeUpdate();
	}
		
	private void removeDeletedEntriesFromIndex(Vector<EntryInfo> vDeletedEntries) throws Exception
	{
		createIndexWriters();

		for (Enumeration<EntryInfo> deletedEntries = vDeletedEntries.elements(); deletedEntries.hasMoreElements();)
		{
			EntryInfo entry = deletedEntries.nextElement();
			removeEntryFromIndex(entry);
		}
		
		closeIndexWriters();
	}
	
	private Vector<Integer> getEntriesPointingAtEntries(Vector<EntryInfo> vDeletedEntries) throws SQLException
	{
		pstEntriesPointingAtEntry = cnRecommender.prepareStatement("SELECT entry_id FROM recommendations WHERE dest_entry_id = ?");

		Vector<Integer> vEntriesToUpdate = new Vector<Integer>();
		
		for (Enumeration<EntryInfo> deletedEntries = vDeletedEntries.elements(); deletedEntries.hasMoreElements();)
		{
			EntryInfo entry = deletedEntries.nextElement();
			getIDsOfEntriesPointingAtEntry(vEntriesToUpdate, entry.nEntryID);
		}
		pstEntriesPointingAtEntry.close();
		return vEntriesToUpdate;
	}
	
	private void deleteRecommendationsInvolvingEntries(Vector<EntryInfo> vDeletedEntries) throws Exception
	{
		pstDeleteEntryRecommendations = cnRecommender.prepareStatement(
			"DELETE FROM recommendations WHERE dest_entry_id = ? OR entry_id = ?");

		pstDeleteEntry = cnRecommender.prepareStatement(
			"DELETE FROM entries WHERE id = ?");
		
		for (Enumeration<EntryInfo> deletedEntries = vDeletedEntries.elements(); deletedEntries.hasMoreElements();)
		{
			EntryInfo entry = deletedEntries.nextElement();
			deleteRecommendationsInvolvingEntry(entry.nEntryID);
		}
		pstDeleteEntryRecommendations.close();
		pstDeleteEntryRecommendations = null;
		pstDeleteEntry.close();
		pstDeleteEntry = null;
	}

	private void updateIndex(boolean bAll)
	{
		try
		{
			createIndexWriters();

			try
			{
				int nEntry = 0;
				
				pstFlagEntryIndexed = cnRecommender.prepareStatement(
					"UPDATE entries SET indexed_at = now() WHERE id = ?");
		
				PreparedStatement pstEntryToIndex = cnRecommender.prepareStatement(
						"SELECT entries.id, entries.feed_id, permalink, direct_link, entries.title, entries.description, " + 
						"tag_list, entries.language, feeds.short_title AS collection " + 
						"FROM entries " +
						"INNER JOIN feeds ON entries.feed_id = feeds.id " +
						"WHERE entries.id = ?");
				
				Vector<Integer> vIDs = getIDsOfEntries(bAll ? "WHERE substring(language,1,2) IN ('en', 'es', 'zh', 'fr', 'ja', 'de', 'ru', 'nl')" : "WHERE harvested_at > indexed_at AND substring(language,1,2) IN ('en', 'es', 'zh', 'fr', 'ja', 'de', 'ru', 'nl')");
				Logger.status("updateIndex - begin (entries to update): " + vIDs.size());
				for (Enumeration<Integer> eIDs = vIDs.elements(); eIDs.hasMoreElements();)
				{
					pstEntryToIndex.setInt(1, eIDs.nextElement().intValue());
					ResultSet rsEntryToIndex = pstEntryToIndex.executeQuery();
					if (rsEntryToIndex.next())
					{
						EntryInfo entry = new EntryInfo(rsEntryToIndex);
						entry.sFeedShortTitle = rsEntryToIndex.getString("collection");
						indexEntry(entry);
						nEntry++;
					}
					rsEntryToIndex.close();
					if (nEntry % 100 == 0)
					{
						pstFlagEntryIndexed.executeBatch();
					}
					if (nEntry % 1000 == 0)Logger.status("Indexing: " + nEntry);
					else if (nEntry % 100 == 0)Logger.info("Indexing: " + nEntry);
				}
				pstEntryToIndex.close();
				pstFlagEntryIndexed.executeBatch();
				pstFlagEntryIndexed.close();
			}
			catch (SQLException e)
			{
				Logger.error("updateIndex(1) - ", e);
			}
			catch (Exception e)
			{
				Logger.error("updateIndex(2) - ", e);
			}
			closeIndexWriters();
		}
		catch (Exception e)
		{
			Logger.error("updateIndex(3) - ", e);
		}
		Logger.status("updateIndex - end");
	}
	
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
        java.util.logging.Logger.getLogger(org.apache.solr.core.MultiCore.class.getName()).setLevel(level);
        java.util.logging.Logger.getLogger(org.apache.solr.update.DirectUpdateHandler2.class.getName()).setLevel(level);
        java.util.logging.Logger.getLogger(org.apache.solr.core.Config.class.getName()).setLevel(level);
        java.util.logging.Logger.getLogger(org.apache.solr.update.UpdateHandler.class.getName()).setLevel(level);
        
	}
	
	private void loadOptions()
	{
		Properties properties = new Properties();
	    try 
	    {
	    	// load the property file
	    	FileInputStream in = new FileInputStream("recommenderd.properties");
	        properties.load(in);
	        in.close();
	        
	        // give the logger a chance to get its options
			Logger.getOptions(properties);

			// get some options out of it
	        String sValue = properties.getProperty("max_recommendations");
	        if (sValue != null) nMaxRecommendations = Integer.parseInt(sValue);
	        sValue = properties.getProperty("solr_dir");
	        if (sValue != null) sSolrDir = sValue;
	        sValue = properties.getProperty("smtp_server");
	        if (sValue != null) sSMTPServer = sValue;
	        sValue = properties.getProperty("admin_email");
	        if (sValue != null) sAdminEmail = sValue;
	    }
	    catch(Exception e){Logger.error(e);}
	    
	    configSolrLogging();
	}
	
	private void initMultiCore() throws Exception 
	{
		// since SolrDispatchFilter can be derived & initMultiCore can be overriden
		mcore = org.apache.solr.core.SolrMultiCore.getInstance();
		if (mcore.isEnabled()) 
		{
			Logger.info("Using existing multicore configuration");
		} 
		else 
		{
			// multicore load
			File fconf = new File(sSolrDir, "multicore.xml");
			Logger.info("looking for multicore.xml: " + fconf.getAbsolutePath());
			mcore.load(sSolrDir, fconf);
		}
	}

	private void closeCores()
	{
		for (Enumeration<SolrCore> eCores = vCores.elements(); eCores.hasMoreElements();)
		{
			eCores.nextElement().close();
		}
		vCores.removeAllElements();
		vCores = null;
	}

	private void closeIndexWriters() throws Exception
	{
		for (Enumeration<IndexWriter> eWriters = htWriters.elements(); eWriters.hasMoreElements();)
		{
			IndexWriter writer = eWriters.nextElement();
			writer.optimize();
			writer.close();
		}
		htWriters.clear();
		htWriters = null;
	}

	private void closeIndexSearchers() throws Exception
	{
		for (Enumeration<IndexSearcher> eSearchers = htSearchers.elements(); eSearchers.hasMoreElements();)
		{
			eSearchers.nextElement().close();
		}
		htSearchers.clear();
		htSearchers = null;
	}
	
	private void closeIndexReaders() throws Exception
	{
		for (Enumeration<IndexReader> eReaders = htReaders.elements(); eReaders.hasMoreElements();)
		{
			eReaders.nextElement().close();
		}
		htReaders.clear();
		htReaders = null;
	}
	
	private void createAnalyzer(SolrCore core)
	{
		vCores.add(core);
		htAnalyzers.put(core.getName(), core.getSchema().getAnalyzer());
	}
	
	private void createAnalyzers() throws Exception
	{
		initMultiCore();
		
		vCores = new Vector<SolrCore>();
		htAnalyzers = new Hashtable<String, org.apache.lucene.analysis.Analyzer>();

		for (Iterator<SolrCore> eCores = mcore.getCores().iterator(); eCores.hasNext();)
		{
			createAnalyzer(eCores.next());
		}
	}
	
	private void createIndexWriters() throws Exception
	{
		htWriters = new Hashtable<String, IndexWriter>();
		for (Enumeration<SolrCore> eCores = vCores.elements(); eCores.hasMoreElements();)
		{
			SolrCore core = eCores.nextElement();
			String sName = core.getName();
			String test=core.getIndexDir();
			htWriters.put(sName, new IndexWriter(core.getIndexDir(), htAnalyzers.get(sName)));
			
		}
	}
	
	private void createIndexReaders() throws Exception
	{
		htReaders = new Hashtable<String, IndexReader>();
		for (Enumeration<SolrCore> eCores = vCores.elements(); eCores.hasMoreElements();)
		{
			SolrCore core = eCores.nextElement();
			htReaders.put(core.getName(), IndexReader.open(FSDirectory.getDirectory(core.getIndexDir())));
		}
	}
	
	private void createIndexSearchers() throws Exception
	{
		htSearchers = new Hashtable<String, IndexSearcher>();
		for (Enumeration<SolrCore> eCores = vCores.elements(); eCores.hasMoreElements();)
		{
			SolrCore core = eCores.nextElement();
			htSearchers.put(core.getName(), new IndexSearcher(htReaders.get(core.getName())));
		}
	}
	
	// TODO: Move to the harvester
	private void updateLanguageRecordCounts() 
	{
		try
		{
			Statement st = cnRecommender.createStatement();
			st.executeUpdate("UPDATE feeds SET entries_count = (SELECT count(*) FROM entries WHERE feed_id = feeds.id)");
			st.executeUpdate("UPDATE languages SET indexed_records = (SELECT count(*) FROM entries WHERE entries.language = languages.code)");
			st.close();
		}
		catch (SQLException e)
		{
			Logger.error("updateLanguagesRecordCount", e);
		}
	}
	
//	private void testLucene() throws Exception
//	{
//		createIndexWriters();
//
//		EntryInfo entry = new EntryInfo();
//		entry.nEntryID = 1;
//		entry.nFeedID = 1;
//		entry.sLanguage = "en";
//		entry.sURI = "http://localhost/";
//		entry.sTitle = "Title";
//		entry.sFeedShortTitle = "MIT";
//		entry.sDescription = "Alvirne's AP Calculus Class invites you to join them in their preparation for the AP exam. In addition to their own <a href=\"http://www.seresc.k12.nh.us/www/currpb.html\">Problem of the Week</a> and a <a href=\"http://www.seresc.k12.nh.us/www/guestpb.html\">Guest Problem of the Week,</a> these students maintain links to teacher and student calculus resources on the Internet; post information about the AP Calculus exam; and maintain archives of previous Alvirne and guest problems dating back to 1995, with detailed solutions.";
//		entry.sTagList = "Tag";
//		indexEntry(entry);
//		
////		entry.nEntryID = 2;
////		indexEntry(entry);
////		
////		entry.nEntryID = 3;
////		entry.sTitle = "Testing Lucene is Not as Fun as You Might Think!";
////		indexEntry(entry);
//		
//		closeIndexWriters();
//		
//		createIndexReaders();
//		createIndexSearchers();
//		entry.nEntryID = 1;
//		getRelatedEntries(entry);
//		closeIndexSearchers();
//		closeIndexReaders();
//	}
	
	private void updateForDeletedEntries(boolean bRedoAllRecommendations) throws Exception
	{
		Vector<EntryInfo> vEntries = getDeletedEntries();
		if (vEntries.size() == 0) return;
		
		Logger.status("Deleting entries: " + vEntries.size());
		
		try
		{
			removeDeletedEntriesFromIndex(vEntries);
			Vector<Integer> vEntriesToUpdate = null;
			if (!bRedoAllRecommendations) vEntriesToUpdate = getEntriesPointingAtEntries(vEntries); 
			deleteRecommendationsInvolvingEntries(vEntries);
			if (!bRedoAllRecommendations)
			{
				Logger.status("Redoing recommendations for entries that pointed at entries that were deleted: " + vEntriesToUpdate.size());
				updateRecommendations(vEntriesToUpdate);
			}
		}
		catch (Exception e)
		{
			Logger.error("updateForDeletedEntries", e);
		}
	}
	
	private boolean isEntryInDB(int nEntryID) throws SQLException
	{
		pstIsEntryInDB.setInt(1, nEntryID);
		ResultSet rs = pstIsEntryInDB.executeQuery();
		boolean bInDB = rs.next();
		rs.close();
		return bInDB;
	}
	
	private PreparedStatement pstIsEntryInDB;
	
	private Vector<EntryInfo> getIndexEntriesNotInDB() throws Exception
	{
		pstIsEntryInDB = cnRecommender.prepareStatement("SELECT id FROM entries WHERE id = ?");
		Vector<EntryInfo> vEntries = new Vector<EntryInfo>();
		createIndexReaders();
		for (Enumeration<String> eReaders = htReaders.keys(); eReaders.hasMoreElements();)
		{
			String sLanguage = eReaders.nextElement();
			IndexReader reader = htReaders.get(sLanguage);
			for (int nDoc = 0; nDoc < reader.maxDoc(); nDoc++)
			{
				Document doc = reader.document(nDoc);
				int nEntryID = Integer.parseInt(doc.get("pk_i"));
				if (!isEntryInDB(nEntryID))
				{
					EntryInfo entry = new EntryInfo();
					entry.nEntryID = nEntryID;  
					entry.sLanguage = sLanguage; 
					vEntries.add(entry);
				}
			}
		}
		closeIndexReaders();
		pstIsEntryInDB.close();
		return vEntries;
	}
	
	private void cleanupIndex()
	{
		try
		{
			Vector<EntryInfo> vEntries = getIndexEntriesNotInDB();
			if (vEntries.size() > 0)
			{
				Logger.status("Deleting entries in index but not in DB: " + vEntries.size());
				removeDeletedEntriesFromIndex(vEntries);
			}
		}
		catch (Exception e)
		{
			Logger.error("cleanupIndex ", e);
		}
	}
	
	private void processDocuments(boolean bRedoAllRecommendations, boolean bReIndexAll)
	{
		loadOptions();

		Logger.status("processDocuments - begin");
		
		// use the aggregator to get any new records
		boolean bChanges = Harvester.harvest();
		//boolean bChanges=true;
		if (!bReIndexAll && !bChanges && !bRedoAllRecommendations) return;
		
		try
		{
			cnRecommender = getConnection("recommender");
			
			createAnalyzers();
			
			// update indexes and db for deleted records (OAI)
			updateForDeletedEntries(bRedoAllRecommendations);
				
			// index any new records
			updateIndex(bReIndexAll);
			
			// create recommendations just for the new records, or for all
			updateRecommendations(bRedoAllRecommendations);
			
			// create recommendations for new users or update recommendations for old ones
			updatePersonalRecommendations(bRedoAllRecommendations);
			
			// update tag lists
			if (bChanges) updateTags();
			if (bChanges) updateQueries();
			
			// close the lucene indexes
			closeCores();
			
			if (bChanges) updateLanguageRecordCounts();
			
			cnRecommender.close();
		}
		catch(Exception e){Logger.error(e);}

		Logger.status("processDocuments - end");

		if (sSMTPServer != null) notifyAdminOfResults();
	}
	
	final static String sEmailFrom = "oerrecomender@cosl.usu.edu";
	final static String sReportSubject = "OER Recommender Harvest Report";
	public void notifyAdminOfResults()
	{
		String sBody = Harvester.getLogMessages() + Logger.getMessages();
		SendMail.sendMsg(sSMTPServer, sEmailFrom, sAdminEmail, sReportSubject, sBody);
	}
	
	
	public static void update(String sAction)
	{
		new Recommender().processDocuments("full".equals(sAction), "reindex".equals(sAction));
	}
	
	public static void main(String[] args) 
	{
		update(args.length > 0 ? args[0] : "");
		System.exit(0) ;
	}
}
