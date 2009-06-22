package edu.usu.cosl.recommenderd;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;

import java.util.Enumeration;
import java.util.Properties;
import java.util.Arrays;
import java.util.Vector;
import java.util.Hashtable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Collections;

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

import org.apache.solr.core.CoreContainer;
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
	private PreparedStatement pstAddPersonalRec;
	
	private PreparedStatement pstGetSubjectID;
	private PreparedStatement pstAddSubject;
	private PreparedStatement pstAddEntrySubject;
	private int nNewEntrySubjects;
	
	private CoreContainer mcore;
	private Vector<SolrCore> vCores;
	private Hashtable<String, Analyzer> htAnalyzers;
	private Hashtable<String, IndexWriter> htWriters;
	private Hashtable<String, IndexReader> htReaders;
	private Hashtable<String, IndexSearcher> htSearchers;
	private int nMaxRecommendations = 20;
	private String sSMTPServer = null;
	private String sAdminEmail = null;
	private Hashtable<Integer, String> htIDToLanguage = new Hashtable<Integer, String>();
	private Hashtable<String, Integer> htLanguageToID = new Hashtable<String, Integer>();

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
			Analyzer analyzer = htAnalyzers.get(htIDToLanguage.get(entry.nLanguageID));
		    if (analyzer == null) return vEntries;
		    
		    // find the lucene document for the entry
			TermQuery query = new TermQuery(new Term("id","Entry:" + entry.nEntryID));
			IndexSearcher searcher = htSearchers.get(htIDToLanguage.get(entry.nLanguageID));
		    Hits hits = searcher.search(query);
		    int nDocID = hits.id(0);
		    if (hits.length() == 0 || nDocID == 0) return vEntries;
		    
		    // ask lucene for more entries like this on
		    IndexReader reader = htReaders.get(htIDToLanguage.get(entry.nLanguageID));
		    MoreLikeThis mlt = new MoreLikeThis(reader);
		    mlt.setMinTermFreq(1);
		    mlt.setAnalyzer(analyzer);
		    mlt.setFieldNames(new String[]{"text"});
		    mlt.setMinWordLen(("zh".equals(htIDToLanguage.get(entry.nLanguageID)) || "ja".equals(htIDToLanguage.get(entry.nLanguageID))) ? 1 : 4);
		    mlt.setMinDocFreq(2);
		    mlt.setBoost(true);
		    Query like = mlt.like(nDocID);
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
		Vector<Integer> vIDs = getIDsOfEntries(bAll ? "":"WHERE indexed_at > relevance_calculated_at");
		if (vIDs.size() > 0) {
			Logger.status("updateRecommendations - begin (entries to update): " + vIDs.size());
			updateRecommendations(vIDs);
			Logger.status("updateRecommendations - end");
		}
	}
	private void updatePersonalRecommendations(boolean bAll) throws SQLException
	{
		Logger.info("updatePersonalRecommendations-begin");
		
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
		
		Logger.info("updatePersonalRecommendations-end");
	}
	private void updateRecommendations(Vector<Integer> vIDs)
	{
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
					"SELECT id, feed_id, permalink, direct_link, title, description, language_id " +
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
	
	private void autoGenerateSubjects(HashSet hsSubjects, int nEntryID, int nSubjects, IndexReader reader, IndexSearcher searcher)
	{
		try {
			// find the document
			TermQuery query = new TermQuery(new Term("id", "Entry:" + nEntryID));
		    Hits hits = searcher.search(query);
		    if (hits.length() > 0) 
		    {
				// get its unstemmed terms
			    int nDocID = hits.id(0);
	    		TermFreqVector tfv = reader.getTermFreqVector(nDocID, "tag");
	    	    if (tfv != null)
	    	    {
		    	    String[] asTerms = tfv.getTerms();
		    	    int[] anFrequencies = tfv.getTermFrequencies();
		    	    TermFrequency[] atf = new TermFrequency[asTerms.length];
	    	    	for (int nTerm = 0; nTerm < asTerms.length; nTerm++) {
	    	    		atf[nTerm] = new TermFrequency(asTerms[nTerm], anFrequencies[nTerm]);
	    	    	}
	    	    	Arrays.sort(atf);

	    	    	for (int nTerm = 0; nTerm < asTerms.length && nSubjects < 5 && atf[nTerm].nFrequency > 2; nTerm++)
	    	    	{
	    	    		nSubjects += addEntrySubject(hsSubjects, nEntryID, atf[nTerm].sTerm);
	    	    	}
	    	    }
		    }
		} catch (Exception e) {
			Logger.error("autoGenerateSubjects-error: ", e);
		}
	}
	
	private int addEntrySubject(HashSet hsSubjects, int nEntryID, String sSubject) throws SQLException
	{
		int nAddedSubjects = 0;
		
		// TODO: use the version of this method that takes a locale
		sSubject = sSubject.toLowerCase();
		if (sSubject.length() > 0)
		{
			String[] asSubjects = sSubject.split("--|[:;,\\)(/\".]");
			for (int nSubject = 0; nSubject < asSubjects.length; nSubject++)
			{
				String sNormalizedSubject = asSubjects[nSubject].trim();
				if (sNormalizedSubject.length() > 0 && !hsSubjects.contains(sNormalizedSubject)) {
					pstAddEntrySubject.setInt(1, getSubjectID(sNormalizedSubject));
					pstAddEntrySubject.setInt(2, nEntryID);
					pstAddEntrySubject.addBatch();
					nNewEntrySubjects++;
					if (nNewEntrySubjects > 100) {
						pstAddEntrySubject.executeBatch();
						nNewEntrySubjects = 0;
					}
					nAddedSubjects++;
				}
			}
		}
		return nAddedSubjects;
	}
	
	private int getSubjectID(String sSubject) throws SQLException
	{
		// set up the prepared statement
		pstGetSubjectID.setString(1, sSubject);
		
		// do the query
		ResultSet rs = pstGetSubjectID.executeQuery();
		
		// if the subject isn't already in the database, add it now
		int nSubjectID = 0;
		if (!rs.next()) nSubjectID = addSubject(sSubject);
		
		// return the subject's id
		else nSubjectID = rs.getInt(1);

		rs.close();
		return nSubjectID;
	}
	
	private int addSubject(String sSubject) throws SQLException
	{
		pstAddSubject.setString(1, sSubject);
		pstAddSubject.executeUpdate();
		return getLastID(pstAddSubject);
	}
	
	private int getLastID(Statement st) throws SQLException
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
	
	private void autoGenerateSubjectsForEntries() throws Exception
	{
		Logger.status("autoGenerateSubjectsForEntries - begin");
		
		PreparedStatement pstNukeOldAutoSubjects = cnRecommender.prepareStatement("DELETE FROM entries_subjects WHERE entry_id = ? AND autogenerated = true");
		PreparedStatement psGetEntrySubjects = cnRecommender.prepareStatement("SELECT subjects.name FROM entries_subjects INNER JOIN subjects ON entries_subjects.subject_id = subjects.id WHERE entries_subjects.entry_id = ? LIMIT 5");
		pstGetSubjectID = cnRecommender.prepareStatement("SELECT id FROM subjects WHERE name = ?");
		pstAddSubject = cnRecommender.prepareStatement("INSERT INTO subjects (name) VALUES (?)");
		pstAddEntrySubject = cnRecommender.prepareStatement("INSERT INTO entries_subjects (subject_id, entry_id, autogenerated) VALUES (?, ?, true)");
		nNewEntrySubjects = 0;
		
		// loop through each of the indexes (languages)
		for (Enumeration<SolrCore> eCores = vCores.elements(); eCores.hasMoreElements();) 
		{
			// get the core
			SolrCore core = eCores.nextElement();
			String sLanguageCode = core.getName();
			
			// open a reader and searcher on the index 
			IndexReader reader = IndexReader.open(FSDirectory.getDirectory(core.getIndexDir()));
			IndexSearcher searcher = new IndexSearcher(reader);
			
			// ask the db for the entries that have been updated or added for the language
			Vector<Integer> vIDs = getIDsOfEntries("WHERE indexed_at > relevance_calculated_at AND language_id = " + htLanguageToID.get(sLanguageCode));
			Logger.info("Generating subjects for entries (" + sLanguageCode + "): " + vIDs.size());
			for(Enumeration<Integer> eID = vIDs.elements(); eID.hasMoreElements();)
			{
				int nEntryID = eID.nextElement();

				// delete any autogenerated subjects for this entry 
				pstNukeOldAutoSubjects.setInt(1,nEntryID);
				pstNukeOldAutoSubjects.executeUpdate();

				// for entries that don't have at least 5 subjects, generate some
				psGetEntrySubjects.setInt(1, nEntryID);
				ResultSet rsEntries = psGetEntrySubjects.executeQuery();
				HashSet<String> hsSubjects = new HashSet<String>();
				int nSubjects = 0;
				while (rsEntries.next()) {
					hsSubjects.add(rsEntries.getString(1));
					nSubjects++;
				}
				rsEntries.close();
				if (nSubjects < 5) {
					autoGenerateSubjects(hsSubjects, nEntryID, nSubjects, reader, searcher);
				}
			}
			// close the index searcher and reader
			searcher.close();
			reader.close();
		}
		psGetEntrySubjects.close();
		pstNukeOldAutoSubjects.close();
		if (nNewEntrySubjects > 0) {
			pstAddEntrySubject.executeBatch();
		}
		pstGetSubjectID.close();
		pstAddSubject.close();
		pstAddEntrySubject.close();

		Logger.status("autoGenerateSubjectsForEntries - end");
	}
	
	private void updateTagClouds() throws Exception
	{
		Logger.status("Update tag clouds - begin");
		try
		{
			autoGenerateSubjectsForEntries();
			
			TagCloud.updateClouds(cnRecommender, htIDToLanguage);
		}
		catch (Exception e) {
			Logger.error("updateTagClouds-error: ", e);
		}
		Logger.status("Update tag clouds - end");
	}

	private void updateQueries()
	{
		Logger.info("updateQueries - begin");

		try
		{
			Statement stSubjectFreq=cnRecommender.createStatement();
			ResultSet subjectFreq=stSubjectFreq.executeQuery("select subject_id, count(*) from taggings group by subject_id");
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
		Logger.info("updateQueries - end");
	}

	private void indexEntry(EntryInfo entry) throws Exception 
	{
		// don't put into lucene entries whose language we don't have an analyzer for
		Analyzer analyzer = htAnalyzers.get(htIDToLanguage.get(entry.nLanguageID));
		if (analyzer != null)
		{
			htWriters.get(htIDToLanguage.get(entry.nLanguageID)).updateDocument(new Term("id","Entry:" + entry.nEntryID), LuceneDocument.Document(entry), analyzer);
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
//		Logger.info("Entries to process: " + vIDs.size());
		return vIDs;
	}

	private void removeEntryFromIndex(EntryInfo entry) throws Exception
	{
		// don't put into lucene entries whose language we don't have an analyzer for
		htWriters.get(htIDToLanguage.get(entry.nLanguageID)).deleteDocuments(new Term("id","Entry:" + entry.nEntryID));
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
		ResultSet rsEntries = stEntries.executeQuery("SELECT id, language_id FROM entries WHERE oai_identifier = 'deleted'");
		while (rsEntries.next())
		{
			EntryInfo entry = new EntryInfo();
			entry.nEntryID = rsEntries.getInt(1);
			entry.nLanguageID = rsEntries.getInt(2);
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
						"entries.language_id, feeds.short_title AS collection " + 
						"FROM entries " +
						"INNER JOIN feeds ON entries.feed_id = feeds.id " +
						"WHERE entries.id = ?");
				
				//Vector<Integer> vIDs = getIDsOfEntries(bAll ? "WHERE substring(language,1,2) IN ('en', 'es', 'zh', 'fr', 'ja', 'de', 'ru', 'nl')" : "WHERE harvested_at > indexed_at AND substring(language,1,2) IN ('en', 'es', 'zh', 'fr', 'ja', 'de', 'ru', 'nl')");
				Vector<Integer> vIDs = getIDsOfEntries(bAll ? "": "WHERE harvested_at > indexed_at");
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
        java.util.logging.Logger.getLogger(org.apache.solr.core.CoreContainer.class.getName()).setLevel(level);
        java.util.logging.Logger.getLogger(org.apache.solr.update.DirectUpdateHandler2.class.getName()).setLevel(level);
        java.util.logging.Logger.getLogger(org.apache.solr.core.Config.class.getName()).setLevel(level);
        java.util.logging.Logger.getLogger(org.apache.solr.update.UpdateHandler.class.getName()).setLevel(level);
	}
	
	private boolean bTest = false;
	private boolean bHarvest = true;
	private boolean bRedoAllRecommendations = false;
	private boolean bReIndexAll = false;
	
	private void loadOptions()
	{
		Properties properties = new Properties();
	    try 
	    {
	    	// load the property file
	    	FileInputStream in = new FileInputStream("recommenderd.properties");
	        properties.load(in);
	        in.close();
	        
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
	        sValue = properties.getProperty("test_mode");
	        if (sValue != null) bTest = "true".equals(sValue);
	        sValue = properties.getProperty("harvest");
	        if (sValue != null) bHarvest = "true".equals(sValue);
	        sValue = properties.getProperty("redo_recommendations");
	        if (sValue != null) bRedoAllRecommendations = "true".equals(sValue);
	        sValue = properties.getProperty("reindex_all");
	        if (sValue != null) bReIndexAll = "true".equals(sValue);
	        
	        getDBOptions(properties);
	    }
	    catch(Exception e){Logger.error(e);}
	    
	    configSolrLogging();
	}
	
	private void initMultiCore() throws Exception 
	{
		org.apache.solr.core.CoreContainer.Initializer initializer = new org.apache.solr.core.CoreContainer.Initializer();
		mcore = initializer.initialize();
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
					entry.nLanguageID = htLanguageToID.get(sLanguage); 
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
	
	final static String sEmailFrom = "oerrecomender@cosl.usu.edu";
	final static String sReportSubject = "OER Recommender Harvest Report";
	public void notifyAdminOfResults()
	{
		SendMail.sendMsg(sSMTPServer, sEmailFrom, sAdminEmail, sReportSubject, Logger.getMessages());
	}
	public void getLanguageMappings(Connection cn)
    {
    	try{
	    	PreparedStatement pstGetSupportLanguages=cn.prepareStatement("SELECT id, locale, is_default FROM languages where muck_raker_supported=1");
	    	ResultSet result=pstGetSupportLanguages.executeQuery();
	    	while(result.next()){
	    		Integer nLanguageID = result.getInt(1);
	    		String sLocale = result.getString(2).substring(0,2);
	    		htIDToLanguage.put(nLanguageID,sLocale);
	    		htLanguageToID.put(sLocale,nLanguageID);
	    	}
    	}
    	catch(Exception e){
    		Logger.error("Read from table language");
    	}
    }
	
//	private void generateTagClouds()
//	{
//		loadOptions();
//
//		Logger.status("generateTagClouds - begin");
//		
//		try
//		{
//			cnRecommender = getConnection();
//			getLanguageMappings(cnRecommender);
//			createAnalyzers();
//			updateTagClouds();
//			closeCores();
//			cnRecommender.close();
//		}
//		catch(Exception e)
//		{
//			Logger.error(e);
//		}
//	}
	
	private void processDocuments()
	{
		Logger.status("processDocuments - begin");
		
		// use the aggregator to get any new records
		boolean bChanges = true;
		if (bHarvest) bChanges = Harvester.harvest(bTest);
		if (!bReIndexAll && !bChanges && !bRedoAllRecommendations) return;
		
		try
		{
			cnRecommender = getConnection();
			
			// get supported languages
			getLanguageMappings(cnRecommender);
			
			createAnalyzers();
			
			// update indexes and db for deleted records (OAI)
			updateForDeletedEntries(bRedoAllRecommendations);
				
			// index any new records
			updateIndex(bReIndexAll);
			
			// update tag clouds
			updateTagClouds();
//			if (bChanges) updateQueries();
			
			// create recommendations just for the new records, or for all
			updateRecommendations(bRedoAllRecommendations);
			
			// create recommendations for new users or update recommendations for old ones
			updatePersonalRecommendations(bRedoAllRecommendations);
			
			// close the lucene indexes
			closeCores();
			
			cnRecommender.close();
		}
		catch(Exception e){Logger.error(e);}

		Logger.status("processDocuments - end");

		if (sSMTPServer != null) notifyAdminOfResults();
		
		Logger.setLogFilePrefix(null);
	}
	
	public static void update(String sAction)
	{
		Recommender r = new Recommender();
		r.loadOptions();
		if (sAction.equals("skip_harvest")) r.bHarvest = false;
		r.processDocuments();
	}
	
	public static void main(String[] args) 
	{
//		new Recommender().generateTagClouds();
		update(args.length > 0 ? args[0] : "");
	}
}
