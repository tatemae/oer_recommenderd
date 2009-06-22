package edu.usu.cosl.recommenderd;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import java.util.Arrays;
import java.util.Enumeration;

import edu.usu.cosl.util.Logger;
import edu.usu.cosl.util.DBThread;

class TagCloud extends DBThread
{
	static Connection cn;
	static PreparedStatement pstAddTagCloud;
	static PreparedStatement pstGetTagCloud;
	static PreparedStatement pstGetFilteredTagCloud;
	static int nUpdatedClouds = 0;
	
	int nTags;
	double dMin;
	double dMax;
	TermFrequency[] atf;
	int nLanguageID;
	int nMaxTags;
	String sFilter;
	
	public TagCloud(int nLanguageID, int nMaxTags)
	{
		this(nLanguageID, nMaxTags, null);
	}
	public TagCloud(int nLanguageID, int nMaxTags, String sFilter)
	{
		this.nLanguageID = nLanguageID;
		this.nMaxTags = nMaxTags;
		this.atf = new TermFrequency[nMaxTags];
		this.sFilter = sFilter;
	}

	private static void setupPreparedStatements() throws Exception
	{
		if (cn == null && pstGetFilteredTagCloud == null)
		{
			cn = getConnection();
			pstGetFilteredTagCloud = cn.prepareStatement(sFilteredTagCloudSQL);
			pstGetTagCloud = cn.prepareStatement(sTagCloudSQL);
			pstAddTagCloud = cn.prepareStatement("REPLACE INTO tag_clouds SET language_id = ?, filter = ?, tag_list = ?");
		}
	}
	private static void closePreparedStatements() throws SQLException
	{
		if (nUpdatedClouds > 0) pstAddTagCloud.executeBatch();
		pstAddTagCloud.close();
		pstGetTagCloud.close();
		cn.close();
		pstAddTagCloud = null;
		pstAddTagCloud = null;
		pstGetTagCloud = null;
		cn = null;
	}
	
	private void getTagCloud(PreparedStatement pstGetTags) throws SQLException
	{
		nTags = 0;
		dMin = 100000;
		dMax = 0;
		ResultSet rsTags = pstGetTags.executeQuery();
		while (rsTags.next()) {
			int nFreq = rsTags.getInt(2);
			if (nFreq < dMin) dMin = nFreq;
			if (nFreq > dMax) dMax = nFreq;
			String sTag = rsTags.getString(1);
			if (sFilter == null || !sFilter.contains(sTag)) {
				atf[nTags] = new TermFrequency(sTag, nFreq, true);
				nTags++;
			}
		}
		rsTags.close();
		
		// sort the tags by frequency
		Arrays.sort(atf, 0, nTags);
	}
	
	private String getTagCloudString()
	{
		StringBuffer sbList = new StringBuffer(2500);
		
		// scale their frequencies and generate a list
		double dTagRange = dMax - dMin;
		final double dStyleRange = 5;
		for (int nTag = 0; nTag < nTags; nTag++) {
			TermFrequency tf = atf[nTag];
			if (nTag > 0) sbList.append(",");
			sbList.append(tf.sTerm);
			sbList.append(",");
			sbList.append(Math.round( ((double)tf.nFrequency - dMin)*dStyleRange/dTagRange ));
		}
		return sbList.toString();
	}

	private void storeTagCloud(String sTagCloud) throws SQLException
	{
		if (sTagCloud.length() > 0) {
			pstAddTagCloud.setInt(1, nLanguageID);
			pstAddTagCloud.setString(2, sFilter == null ? "" : sFilter);
			pstAddTagCloud.setString(3, sTagCloud);
			pstAddTagCloud.addBatch();
		}
		if (nUpdatedClouds % 100) {
			pstAddTagCloud.executeBatch();
			nUpdatedClouds = 0;
		}
	}

	final static String sTagCloudSQL = "SELECT s.name, count(*) AS count " + 
	"FROM entries_subjects AS es " + 
	"INNER JOIN subjects AS s ON es.subject_id = s.id " + 
	"INNER JOIN entries AS e ON es.entry_id = e.id " + "" +
	"WHERE e.language_id = ? " + 
	"GROUP BY es.subject_id " + 
	"ORDER BY count DESC " + 
	"LIMIT ?";
	final static String sFilteredTagCloudSQL = "SELECT s2.name, count(*) AS count " +
	"FROM subjects AS s1 " +
	"INNER JOIN entries_subjects AS es ON s1.id = es.subject_id " +
	"INNER JOIN entries AS e ON es.entry_id = e.id " +
	"INNER JOIN entries_subjects AS es2 ON e.id = es2.entry_id " +
	"INNER JOIN subjects AS s2 ON es2.subject_id = s2.id " +
	"WHERE s1.name = ? AND e.language_id = ? " +
	"GROUP BY es2.subject_id " +
	"ORDER BY count DESC LIMIT ?";

	private PreparedStatement getMultiTagStatement() throws SQLException
	{
		String[] asTags = sFilter.split("/");
		
		StringBuffer sb = new StringBuffer("SELECT subjects.name, COUNT(subjects.id) as count " +    
	    "FROM entries " +
	    "INNER JOIN entries_subjects ON entries_subjects.entry_id = entries.id " +
	    "INNER JOIN subjects ON entries_subjects.subject_id = subjects.id " +
	    "WHERE entries.language_id = " + nLanguageID + " AND (");
	    
		if (asTags.length > 0)
		{
	        sb.append("entries.id IN ");
	        sb.append("(");

	        // The following code finds entries that share the same tags
	        sb.append("SELECT et1.entry_id ");
	        sb.append("FROM entries_subjects AS et1 ");
	        sb.append("INNER JOIN subjects t1 ON t1.id = et1.subject_id ");
	        
	        for (int nTag = 2; nTag < asTags.length + 2; nTag++)
	        {
	            sb.append("INNER JOIN entries_subjects AS et" + nTag + " ON et" + nTag + ".entry_id = et1.entry_id ");
	        	sb.append("INNER JOIN subjects t" + nTag + " ON t" + nTag + ".id = et" + nTag + ".subject_id ");
			}
	        sb.append("WHERE ");
	        
	        String connector = "";
	        for (int nTag = 2; nTag < asTags.length + 2; nTag++)
	        {
	            sb.append(connector + " t" + nTag + ".name = ? ");
	            connector = " AND ";
	        }
	        sb.append(") ) ");
		}
	    sb.append("GROUP BY subjects.id, subjects.name ");
	    sb.append("ORDER BY subjects.name ");
	    String sSql = sb.toString();
//	    Logger.info(sSql);
	    PreparedStatement ps = cn.prepareStatement(sSql);
	    ps.setInt(1, nLanguageID);
        for (int nTag = 0; nTag < asTags.length; nTag++) {
    	    ps.setString(nTag + 1, asTags[nTag]);
        }
        return ps;
	}
	
	private TagCloud updateCloud() throws SQLException
	{
		if (sFilter == null) 
		{
			// get the top tags and their frequencies
			pstGetTagCloud.setInt(1,nLanguageID);
			pstGetTagCloud.setInt(2,nMaxTags);
			getTagCloud(pstGetTagCloud); 
		}
		else if (!sFilter.contains("/"))
		{
			// get the top tags and their frequencies
			pstGetFilteredTagCloud.setString(1,sFilter);
			pstGetFilteredTagCloud.setInt(2,nLanguageID);
			pstGetFilteredTagCloud.setInt(3,nMaxTags);
			getTagCloud(pstGetFilteredTagCloud); 
		}
		else
		{
			getTagCloud(getMultiTagStatement());
		}
		storeTagCloud(getTagCloudString());
		return this;
	}
	
	private static String getFilter(String sTag1, String sTag2)
	{
		if (sTag1.compareTo(sTag2) < 0) return sTag1 + "/" + sTag2;
		else return sTag2 + "/" + sTag1;
	}
	
	private static void updateLanguageClouds(int nLanguageID) throws SQLException
	{
		final int TOP_LEVEL_TAG_COUNT = 200;
		TagCloud tc1 = new TagCloud(nLanguageID, TOP_LEVEL_TAG_COUNT).updateCloud();

		final int SECOND_LEVEL_TAG_COUNT = 100;
		for (int nTag = 0; nTag < tc1.nTags; nTag++)
		{
			TagCloud tc2 = new TagCloud(nLanguageID, SECOND_LEVEL_TAG_COUNT, tc1.atf[nTag].sTerm).updateCloud();
			final int THIRD_LEVEL_TAG_COUNT = 100;
			for (int nTag2 = 0; nTag2 < tc2.nTags; nTag2++)
			{
				new TagCloud(nLanguageID, THIRD_LEVEL_TAG_COUNT, getFilter(tc1.atf[nTag].sTerm, tc2.atf[nTag2].sTerm)).updateCloud();
			}
		}
	}
	
	public static void updateClouds() throws Exception
	{
		Logger.status("Updating tag clouds - begin");
		setupPreparedStatements();
		for (Enumeration<Integer>eLanguageIDs = Locales.getLocaleIDs(); eLanguageIDs.hasMoreElements();)
		{
			int nLanguageID = eLanguageIDs.nextElement();
			Logger.info("Generating tag cloud for language: " + Locales.getCode(nLanguageID));
			TagCloud.updateLanguageClouds(nLanguageID);
		}
		closePreparedStatements();
		Logger.status("Updating tag clouds - end");
	}
	public static void main(String[] args) 
	{
		try {
			getLoggerAndDBOptions("recommenderd.properties");
			updateClouds();
		} catch (Exception e) {
			Logger.error(e);
		}
	}
}
