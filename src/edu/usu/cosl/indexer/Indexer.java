package edu.usu.cosl.indexer;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import java.io.File;

import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Vector;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.solr.core.SolrCore;

import edu.usu.cosl.recommenderd.EntryInfo;
import edu.usu.cosl.util.Locales;
import edu.usu.cosl.recommenderd.Base;
import edu.usu.cosl.subjects.Subject;

public class Indexer extends Base
{
	private PreparedStatement pstFlagEntryIndexed;
	private PreparedStatement pstEntriesPointingAtEntry;
	private PreparedStatement pstDeleteEntryRecommendations;
	private PreparedStatement pstDeleteEntry;
	private Hashtable<String, IndexWriter> htWriters;
	
	private void setupPreparedStatements() throws Exception
	{
		if (cn == null)
		{
			cn = getConnection();
		}
	}
	private void closePreparedStatements() throws SQLException
	{
		if (cn != null)
		{
			cn.close();
			cn = null;
		}
	}
	
	private void createIndexWriters() throws Exception
	{
		htWriters = new Hashtable<String, IndexWriter>();
		for (Enumeration<SolrCore> eCores = vCores.elements(); eCores.hasMoreElements();)
		{
			SolrCore core = eCores.nextElement();
			String sName = core.getName();
			if (!htWriters.containsKey(sName)) {
				File lockFile= new File(core.getIndexDir() + "write.lock");
				if (lockFile.exists()) lockFile.delete();
				htWriters.put(sName, new IndexWriter(core.getIndexDir(), htAnalyzers.get(sName)));
				new File(core.getIndexDir() + "write.lock").deleteOnExit();
			}
		}
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

	private void removeEntryFromIndex(EntryInfo entry) throws Exception
	{
		// don't put into lucene entries whose language we don't have an analyzer for
		htWriters.get(Locales.getCode(entry.nLanguageID)).deleteDocuments(new Term("id","Entry:" + entry.nEntryID));
	}
	
	private void indexEntry(EntryInfo entry) throws Exception 
	{
		String sCode = null;
		try {
		// don't put into lucene entries whose language we don't have an analyzer for
			sCode = Locales.getCode(entry.nLanguageID);
		Analyzer analyzer = htAnalyzers.get(sCode);
		if (analyzer != null) {
			htWriters.get(sCode).updateDocument(new Term("id","Entry:" + entry.nEntryID), LuceneDocument.Document(entry), analyzer);
		}
		pstFlagEntryIndexed.setInt(1, entry.nEntryID);
		pstFlagEntryIndexed.addBatch();
		}catch(Throwable t) {
			logger.error("!!!!!!!!!!!!!" + entry + "," + sCode + "," + entry.nLanguageID + "," + entry.nEntryID);
		}
	}
	
	private void removeDeletedEntriesFromIndex(Vector<EntryInfo> vDeletedEntries) throws Exception
	{
		logger.debug("Removing deleted entries from index:" + vDeletedEntries.size());
		
		createIndexWriters();

		for (Enumeration<EntryInfo> deletedEntries = vDeletedEntries.elements(); deletedEntries.hasMoreElements();)
		{
			EntryInfo entry = deletedEntries.nextElement();
			removeEntryFromIndex(entry);
		}
		
		closeIndexWriters();

		logger.debug("Done deleting entries from index");
	}
	
	private PreparedStatement pstIsEntryInDB;
	
	private Vector<EntryInfo> getIndexEntriesNotInDB() throws Exception
	{
		pstIsEntryInDB = cn.prepareStatement("SELECT id FROM entries WHERE id = ?");
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
					entry.nLanguageID = Locales.getID(sLanguage); 
					vEntries.add(entry);
				}
			}
		}
		closeIndexReaders();
		pstIsEntryInDB.close();
		return vEntries;
	}
	
	private boolean isEntryInDB(int nEntryID) throws SQLException
	{
		pstIsEntryInDB.setInt(1, nEntryID);
		ResultSet rs = pstIsEntryInDB.executeQuery();
		boolean bInDB = rs.next();
		rs.close();
		return bInDB;
	}
	
	private void cleanupIndex()
	{
		try
		{
			Vector<EntryInfo> vEntries = getIndexEntriesNotInDB();
			if (vEntries.size() > 0)
			{
				logger.info("Deleting entries in index but not in DB: " + vEntries.size());
				removeDeletedEntriesFromIndex(vEntries);
			}
		}
		catch (Exception e)
		{
			logger.error("cleanupIndex ", e);
		}
	}
	
	PreparedStatement pstEntrySubjects;

	private Vector<Subject> getSubjects(int nEntryID)
	{
		Vector<Subject> vSubjects = new Vector<Subject>();
		try {
			pstEntrySubjects.setInt(1, nEntryID);
			ResultSet rsSubjects = pstEntrySubjects.executeQuery();
			while (rsSubjects.next()) {
				vSubjects.add(new Subject(rsSubjects.getString(1), rsSubjects.getBoolean(2)));
			}
			rsSubjects.close();
		}catch(Exception e) {
			logger.error("Error in getSubjects: ", e);
		}
		return vSubjects;
	}
	
	private Hashtable<Integer, Vector<Integer>> htFeedAggregations;
	
	private Vector<Integer> getFeedAggregations(int nFeedID) throws SQLException {
		Vector<Integer> vAggregations = new Vector<Integer>();
		Statement st = cn.createStatement();
		ResultSet rs = st.executeQuery("SELECT aggregation_id FROM aggregation_feeds WHERE feed_id = " + nFeedID);
		while (rs.next()) {
			vAggregations.add(rs.getInt(1));
		}
		rs.close();
		st.close();
		return vAggregations;
	}
	
	private void loadFeedAggregations() throws SQLException {
		htFeedAggregations = new Hashtable<Integer, Vector<Integer>>();
		Statement st = cn.createStatement();
		ResultSet rs = st.executeQuery("SELECT id FROM feeds");
		while (rs.next()) {
			Integer feedID = rs.getInt(1);
			htFeedAggregations.put(feedID,getFeedAggregations(feedID));
		}
		rs.close();
		st.close();
	}
	
	private Vector<Integer> getAggregations(int nFeedID) throws SQLException {
		if (htFeedAggregations == null) loadFeedAggregations();
		return htFeedAggregations.get(new Integer(nFeedID));
	}
	
	private void updateIndex(boolean bAll)
	{
		try
		{
			createIndexWriters();

			try
			{
				int nEntry = 0;
				
				pstFlagEntryIndexed = cn.prepareStatement(
					"UPDATE entries SET indexed_at = now() WHERE id = ?");
		
				PreparedStatement pstEntryToIndex = cn.prepareStatement(
						"SELECT entries.id, entries.feed_id, permalink, direct_link, entries.title, entries.description, " + 
						"entries.language_id, feeds.short_title AS collection, entries.grain_size " + 
						"FROM entries " +
						"INNER JOIN feeds ON entries.feed_id = feeds.id " +
						"WHERE entries.id = ?");
				
				pstEntrySubjects = cn.prepareStatement(
						"SELECT name, autogenerated FROM subjects AS s INNER JOIN entries_subjects AS es ON s.id = es.subject_id WHERE es.entry_id = ?");

				//Vector<Integer> vIDs = getIDsOfEntries(bAll ? "WHERE substring(language,1,2) IN ('en', 'es', 'zh', 'fr', 'ja', 'de', 'ru', 'nl')" : "WHERE harvested_at > indexed_at AND substring(language,1,2) IN ('en', 'es', 'zh', 'fr', 'ja', 'de', 'ru', 'nl')");
				String sConditions = "WHERE permalink_good = true AND ";
				Vector<Integer> vIDs = getIDsOfEntries(bAll ? "": "WHERE harvested_at > indexed_at");
				logger.info("updateIndex - begin (entries to update): " + vIDs.size());
				for (Enumeration<Integer> eIDs = vIDs.elements(); eIDs.hasMoreElements();)
				{
					pstEntryToIndex.setInt(1, eIDs.nextElement().intValue());
					ResultSet rsEntryToIndex = pstEntryToIndex.executeQuery();
					if (rsEntryToIndex.next())
					{
						EntryInfo entry = new EntryInfo(rsEntryToIndex);
						entry.sFeedShortTitle = rsEntryToIndex.getString("collection");
						entry.vSubjects = getSubjects(entry.nEntryID);
						entry.vAggregations = getAggregations(entry.nFeedID);
						indexEntry(entry);
						nEntry++;
					}
					rsEntryToIndex.close();
					if (nEntry % 100 == 0)
					{
						pstFlagEntryIndexed.executeBatch();
					}
					if (nEntry % 1000 == 0)logger.info("Indexing: " + nEntry + "/" + vIDs.size());
					else if (nEntry % 100 == 0)logger.debug("Indexing: " + nEntry + "/" + vIDs.size());
				}
				pstEntrySubjects.close();
				pstEntryToIndex.close();
				pstFlagEntryIndexed.executeBatch();
				pstFlagEntryIndexed.close();
			}
			catch (SQLException e)
			{
				logger.error("updateIndex(1) - ", e);
			}
			catch (Exception e)
			{
				logger.error("updateIndex(2) - ", e);
			}
			closeIndexWriters();
		}
		catch (Exception e)
		{
			logger.error("updateIndex(3) - ", e);
		}
	}

	private Vector<Integer> getEntriesPointingAtDeletedEntries(Vector<EntryInfo> vDeletedEntries) throws SQLException
	{
		logger.debug("Getting entries pointed at deleted entries");
		
		pstEntriesPointingAtEntry = cn.prepareStatement("SELECT entry_id FROM recommendations WHERE dest_entry_id = ?");

		Vector<Integer> vEntriesToUpdate = new Vector<Integer>();
		
		for (Enumeration<EntryInfo> deletedEntries = vDeletedEntries.elements(); deletedEntries.hasMoreElements();)
		{
			EntryInfo entry = deletedEntries.nextElement();
			getIDsOfEntriesPointingAtEntry(vEntriesToUpdate, entry.nEntryID);
		}
		pstEntriesPointingAtEntry.close();

		logger.debug("Done getting entries pointed at deleted entries");
		return vEntriesToUpdate;
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

		Statement stEntries = cn.createStatement();
		ResultSet rsEntries = stEntries.executeQuery("SELECT id, language_id, oai_identifier FROM entries WHERE oai_identifier = 'deleted' OR permalink_good = false");
		while (rsEntries.next())
		{
			EntryInfo entry = new EntryInfo();
			entry.nEntryID = rsEntries.getInt(1);
			entry.nLanguageID = rsEntries.getInt(2);
			entry.bDeleted = "deleted".equals(rsEntries.getString(3));
			vEntries.add(entry);
		}
		rsEntries.close();
		stEntries.close();
		return vEntries;
	}

	private void flagNeedUpdatedRecommendations(Vector<Integer> vEntries) throws SQLException
	{
		logger.debug("Flagging entries that had recommendations to entries that were deleted");
		
		StringBuffer sbUpdate = new StringBuffer("UPDATE entries SET relevance_calculated_at = '1971-01-01' WHERE id IN (");
		Enumeration<Integer> eIDs = vEntries.elements();
		while (true)
		{
			sbUpdate.append(eIDs.nextElement());
			if (eIDs.hasMoreElements())sbUpdate.append(", ");
			else break;
		}
		sbUpdate.append(")");
		Statement st = cn.createStatement();
		st.executeUpdate(sbUpdate.toString());
		st.close();

		logger.debug("Done flagging entries that had recommendations to entries that were deleted");
	}
	
	private void updateForDeletedEntries() throws Exception
	{
		Vector<EntryInfo> vEntries = getDeletedEntries();
		if (vEntries.size() == 0) return;
		
		logger.info("Deleting entries: " + vEntries.size());
		
		try
		{
			removeDeletedEntriesFromIndex(vEntries);
			Vector<Integer> vEntriesToUpdate = getEntriesPointingAtDeletedEntries(vEntries); 
			deleteRecommendationsInvolvingEntries(vEntries);
			flagNeedUpdatedRecommendations(vEntriesToUpdate);
		}
		catch (Exception e)
		{
			logger.error("updateForDeletedEntries", e);
		}
		logger.debug("Done deleting entries");
	}
	
	private void deleteRecommendationsInvolvingEntries(Vector<EntryInfo> vDeletedEntries) throws Exception
	{
		logger.debug("Deleting recommendations involving entries");
		
		pstDeleteEntryRecommendations = cn.prepareStatement(
			"DELETE FROM recommendations WHERE dest_entry_id = ? OR entry_id = ?");

		pstDeleteEntry = cn.prepareStatement(
			"DELETE FROM entries WHERE id = ?");
		
		for (Enumeration<EntryInfo> deletedEntries = vDeletedEntries.elements(); deletedEntries.hasMoreElements();)
		{
			EntryInfo entry = deletedEntries.nextElement();
			deleteRecommendationsInvolvingEntry(entry.nEntryID, entry.bDeleted);
		}
		pstDeleteEntryRecommendations.close();
		pstDeleteEntryRecommendations = null;
		pstDeleteEntry.close();
		pstDeleteEntry = null;

		logger.debug("Done deleting recommendations involving entries");
	}
	private void deleteRecommendationsInvolvingEntry(int nEntryID, boolean bDeleteEntry) throws Exception
	{
		// delete recommendations involving the entry being deleted 
		pstDeleteEntryRecommendations.setInt(1, nEntryID);
		pstDeleteEntryRecommendations.setInt(2, nEntryID);
		pstDeleteEntryRecommendations.executeUpdate();
		
		// delete the entry
		if (bDeleteEntry) {
			pstDeleteEntry.setInt(1, nEntryID);
			pstDeleteEntry.executeUpdate();
		}
	}
	
	private Vector<String> getCoreNames() throws Exception{
		Vector<String> vCoreNames = new Vector<String>();
		for (Enumeration<SolrCore> eCores = vCores.elements(); eCores.hasMoreElements();)
		{
			vCoreNames.add(eCores.nextElement().getName());
		}
		return vCoreNames;
	}

//	private void reloadCores() throws Exception{
//		createAnalyzers();
//		Vector<String> vCoreNames = getCoreNames();
//		closeCores();
//		SolrCoreReloader.askSolrToReloadIndexes(vCoreNames);
//	}
	
	public void updateIndexes(boolean bReIndexAll) throws Exception
	{
		logger.debug("==========================================================Index - Begin");
		setupPreparedStatements(); 
		createAnalyzers();
		updateForDeletedEntries();
		updateIndex(bReIndexAll);
		cleanupIndex();
		Vector<String> vCoreNames = getCoreNames();
		closeCores();
		closePreparedStatements();
		SolrCoreReloader.askSolrToReloadIndexes(vCoreNames);
		logger.debug("==========================================================Index - End");
	}
		
	public static void update(boolean bReIndexAll) throws Exception
	{
		new Indexer().updateIndexes(bReIndexAll);
	}
	
	public static void main(String[] args) 
	{
		try {
			getLoggerAndDBOptions("recommenderd.properties");
			update(args.length > 0 && args[0].equals("all"));
		} catch (Exception e) {
			logger.error(e);
		}
	}
}
