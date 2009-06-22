package edu.usu.cosl.recommenderd;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import java.util.Arrays;

class TagList
{
	static PreparedStatement pstAddTagList;
	static PreparedStatement pstGetTagList;
	static PreparedStatement pstGetFilteredTagList;

	int nTags;
	double dMin;
	double dMax;
	TermFrequency[] atf;
	int nLanguageID;
	int nMaxTags;
	String sFilter;
	
	public TagList(int nLanguageID, int nMaxTags)
	{
		this(nLanguageID, nMaxTags, null);
	}
	public TagList(int nLanguageID, int nMaxTags, String sFilter)
	{
		this.nLanguageID = nLanguageID;
		this.nMaxTags = nMaxTags;
		this.atf = new TermFrequency[nMaxTags];
		this.sFilter = sFilter;
	}

	public static void setupPreparedStatements(Connection cnRecommender) throws SQLException
	{
		if (pstGetFilteredTagList == null)
		{
			pstGetFilteredTagList = cnRecommender.prepareStatement(sFilteredTagListSQL);
			pstGetTagList = cnRecommender.prepareStatement(sTagListSQL);
			pstAddTagList = cnRecommender.prepareStatement("REPLACE INTO tag_clouds SET language_id = ?, filter = ?, tag_list = ?");
		}
	}
	public static void closePreparedStatements() throws SQLException
	{
		pstAddTagList.executeBatch();
		pstAddTagList.close();
		pstGetTagList.close();
	}
	
	final static String sTagListSQL = "SELECT s.name, count(*) AS count " + 
	"FROM entries_subjects AS es " + 
	"INNER JOIN subjects AS s ON es.subject_id = s.id " + 
	"INNER JOIN entries AS e ON es.entry_id = e.id " + "" +
	"WHERE e.language_id = ? " + 
	"GROUP BY es.subject_id " + 
	"ORDER BY count DESC " + 
	"LIMIT ?";
	final static String sFilteredTagListSQL = "SELECT s2.name, count(*) AS count " +
	"FROM subjects AS s1 " +
	"INNER JOIN entries_subjects AS es ON s1.id = es.subject_id " +
	"INNER JOIN entries AS e ON es.entry_id = e.id " +
	"INNER JOIN entries_subjects AS es2 ON e.id = es2.entry_id " +
	"INNER JOIN subjects AS s2 ON es2.subject_id = s2.id " +
	"WHERE s1.name=? AND e.language_id = ? " +
	"GROUP BY es2.subject_id " +
	"ORDER BY count DESC LIMIT ?";	
	private TagList updateCloud() throws SQLException
	{
		ResultSet rsTags;
		if (sFilter == null) 
		{
			// get the top tags and their frequencies
			pstGetTagList.setInt(1,nLanguageID);
			pstGetTagList.setInt(2,nMaxTags);
			rsTags = pstGetTagList.executeQuery();
		}
		else
		{
			// get the top tags and their frequencies
			pstGetFilteredTagList.setInt(1,nLanguageID);
			pstGetFilteredTagList.setString(2,sFilter);
			pstGetFilteredTagList.setInt(3,nMaxTags);
			rsTags = pstGetTagList.executeQuery();
		}
		TagList tl = buildTagList(rsTags);
		rsTags.close();
		return tl;
	}
	
	
	private TagList buildTagList(Prepared) throws SQLException
	{
		TagList tl = new TagList(nMaxTags);
		tl.nTags = 0;
		tl.dMin = 100000;
		tl.dMax = 0;
		while (rsTags.next()) {
			int nFreq = rsTags.getInt(2);
			if (nFreq < tl.dMin) tl.dMin = nFreq;
			if (nFreq > tl.dMax) tl.dMax = nFreq;
			tl.atf[tl.nTags] = new TermFrequency(rsTags.getString(1), nFreq, true);
			tl.nTags++;
		}
		// sort the tags by frequency
		Arrays.sort(tl.atf, 0, tl.nTags);
		return tl;
	}
	
	private String getTagList(TagList tl)
	{
		StringBuffer sbList = new StringBuffer(2500);
		
		// scale their frequencies and generate a list
		double dTagRange = tl.dMax - tl.dMin;
		final double dStyleRange = 5;
		for (int nTag = 0; nTag < tl.nTags; nTag++) {
			TermFrequency tf = tl.atf[nTag];
			if (nTag > 0) sbList.append(",");
			sbList.append(tf.sTerm);
			sbList.append(",");
			sbList.append(Math.round( ((double)tf.nFrequency - tl.dMin)*dStyleRange/dTagRange ));
		}
		return sbList.toString();
	}

	private void storeTagList(int nLanguageID, String sFilter, String sTagList) throws SQLException
	{
		if (sTagList.length() > 0) {
			pstAddTagList.setInt(1, nLanguageID);
			pstAddTagList.setString(2, sFilter);
			pstAddTagList.setString(3, sTagList);
			pstAddTagList.addBatch();
		}
	}

	public static void updateLanguageClouds(int nLanguageID) throws SQLException
	{
		final int TOP_LEVEL_TAG_COUNT = 200;
		TagList tl = new TagList(nLanguageID, TOP_LEVEL_TAG_COUNT).updateCloud();

		final int SECOND_LEVEL_TAG_COUNT = 100;
		for (int nTag = 0; nTag < tl.nTags; nTag++)
		{
			new TagList(nLanguageID, SECOND_LEVEL_TAG_COUNT, tl.atf[nTag].sTerm).updateCloud();
		}
	}
}
