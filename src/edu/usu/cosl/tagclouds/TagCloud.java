package edu.usu.cosl.tagclouds;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import java.util.Arrays;
import java.util.Enumeration;
import java.util.Vector;

import edu.usu.cosl.recommenderd.Base;
import edu.usu.cosl.util.Locales;

public class TagCloud extends Base 
{
	int nUpdatedClouds = 0;

	PreparedStatement pstAddTagCloud;
	PreparedStatement pstGetTagCloud;

	int nTags;
	int nMin;
	int nMax;
	TermFrequency[] atf;
	int nLanguageID;
	int nMaxTags;
	String sFilter;

	public TagCloud() {
		atf = new TermFrequency[0];
	}
	public TagCloud(int nLanguageID, int nMaxTags) {
		this(nLanguageID, nMaxTags, null);
	}

	public TagCloud(int nLanguageID) {
		this(nLanguageID, 0, null);
	}

	public TagCloud(int nLanguageID, int nMaxTags, String sFilter) {
		this.nLanguageID = nLanguageID;
		this.nMaxTags = nMaxTags;
		this.atf = new TermFrequency[nMaxTags];
		this.sFilter = sFilter;
//		if (sGrainSize != null) this.sGrainSize = sGrainSize; 
	}

	private void getTagCloud(PreparedStatement pstGetTags) throws SQLException {
		nTags = 0;
		nMin = 1000000;
		nMax = 0;
		ResultSet rsTags = pstGetTags.executeQuery();
		while (rsTags.next()) {
			int nFreq = rsTags.getInt(2);
			if (nFreq < nMin)
				nMin = nFreq;
			if (nFreq > nMax)
				nMax = nFreq;
			String sTag = rsTags.getString(1);
			if (sFilter == null || !sFilter.contains(sTag)) {
				atf[nTags] = new TermFrequency(sTag, nFreq, true);
				nTags++;
			}
		}
		rsTags.close();
		if (nMin == 1000000) nMin = 0;

		// sort the tags by frequency
		Arrays.sort(atf, 0, nTags);
	}

	private String getTagCloudString() {
		StringBuffer sbList = new StringBuffer(5000);

		// scale their frequencies and generate a list
		sbList.append(nMin + "," + nMax);
		for (int nTag = 0; nTag < nTags; nTag++) {
			TermFrequency tf = atf[nTag];
			sbList.append(",");
			sbList.append(tf.sTerm);
			sbList.append(",");
//			sbList.append(Math.round(((double) tf.nFrequency - dMin) * dStyleRange / dTagRange));
			sbList.append(tf.nFrequency);
		}
		return sbList.toString();
	}

	private void storeTagCloud(String sGrainSize, String sTagCloud) throws SQLException {
		if (sTagCloud.length() > 0) {
			pstAddTagCloud.setString(1, sGrainSize);
			pstAddTagCloud.setInt(2, nLanguageID);
			pstAddTagCloud.setString(3, sFilter == null ? "" : sFilter);
			pstAddTagCloud.setString(4, sTagCloud);
//			logger.info("" + sTagCloud.length() + " - " + sTagCloud);
			pstAddTagCloud.addBatch();
		}
		if (nUpdatedClouds % 10 == 0) {
			pstAddTagCloud.executeBatch();
			nUpdatedClouds = 0;
		}
	}

	final static String sTagCloudSQL1 = 
		"SELECT s.name, count(*) AS count "
		+ "FROM entries AS e ";
	final static String sTagCloudSQL2 = 
		" INNER JOIN entries_subjects AS es ON e.id = es.entry_id "
		+ "INNER JOIN subjects AS s ON es.subject_id = s.id "
		+ "WHERE e.language_id = ";
	final static String sTagCloudSQL3 = 
		" GROUP BY s.id " +
		"ORDER BY count DESC " +
		"LIMIT ? ";

	private PreparedStatement getCloudStatement(String sGrainSize) throws Exception
	{
		// build the query
		StringBuffer sbSQL = new StringBuffer(sTagCloudSQL1);
		String[] asFilterTags = (sFilter == null ? new String[0] : sFilter.split("/"));
		for (int nTag = 0; nTag < asFilterTags.length; nTag++) {
			sbSQL.append("INNER JOIN entries_subjects AS es" + nTag + " ON e.id = es" + nTag + ".entry_id ");
			sbSQL.append("INNER JOIN subjects AS s" + nTag + " ON es" + nTag + ".subject_id = s" + nTag + ".id ");
		}
		sbSQL.append(sTagCloudSQL2 + nLanguageID);
		if ("course".equals(sGrainSize))
			sbSQL.append(" AND e.grain_size = 'course' ");
		for (int nTag = 0; nTag < asFilterTags.length; nTag++) {
			sbSQL.append(" AND s" + nTag + ".name = ? ");
		}
		sbSQL.append(sTagCloudSQL3);
//		logger.info(sbSQL.toString());
		PreparedStatement pst = cn.prepareStatement(sbSQL.toString());

		// specify the tag filter and max tags
		int nField = 1;
		for (int nTag = 0; nTag < asFilterTags.length; nTag++) {
			pst.setString(nField++, asFilterTags[nTag]);
		}
		pst.setInt(nField, nMaxTags);
		return pst;
	}
	
	private TagCloud updateCloud(String sGrainSize) throws Exception
	{
		try 
		{
			cn = getConnection();
			pstAddTagCloud = cn.prepareStatement("REPLACE INTO tag_clouds SET grain_size = ?, language_id = ?, filter = ?, tag_list = ?");
	
			logger.info("Cloud: " + Locales.getCode(nLanguageID) + ", "  + sGrainSize + ", " + (sFilter == null ? "" : sFilter));
			
			pstGetTagCloud = getCloudStatement(sGrainSize);
			getTagCloud(pstGetTagCloud);
			storeTagCloud(sGrainSize, getTagCloudString());
	
			if (nUpdatedClouds > 0) pstAddTagCloud.executeBatch();
			pstAddTagCloud.close();
			pstGetTagCloud.close();
			cn.close();
			pstAddTagCloud = null;
			pstAddTagCloud = null;
			pstGetTagCloud = null;
			cn = null;
			return this;
		} 
		catch (Exception e)
		{
			logger.error("updateCloud error: ", e);
			throw e;
		}
	}

	private static String getFilter(String sTag1, String sTag2) {
		if (sTag1.compareTo(sTag2) < 0)
			return sTag1 + "/" + sTag2;
		else
			return sTag2 + "/" + sTag1;
	}

	private static void updateLanguageClouds(int nLanguageID, int nDepth) throws Exception 
	{
		updateGrainSizeLanguageClouds(nLanguageID, "all", nDepth);
		updateGrainSizeLanguageClouds(nLanguageID, "course", nDepth);
	}
	
	static final private String LEVEL1_CLOUD = "SELECT * FROM tag_clouds WHERE language_id = ? AND filter = ''";
	static final private String LEVEL2_CLOUDS = "SELECT * FROM tag_clouds WHERE language_id = ? AND filter NOT like '%/%' AND filter != ''";
	public Vector<TagCloud> getClouds(String sQuery, String sGrainSize) throws Exception
	{
		try
		{
			cn = getConnection();
			if ("course".equals(sGrainSize)) sQuery += " AND grain_size = 'course'";
//			logger.info(sQuery);
			PreparedStatement ps = cn.prepareStatement(sQuery);
			ps.setInt(1, nLanguageID);
			ResultSet rs = ps.executeQuery();
			Vector<TagCloud> vClouds = new Vector<TagCloud>();
			while (rs.next()) {
				TagCloud tc = new TagCloud(nLanguageID);
				String[] asTags = rs.getString("tag_list").split(",");
				tc.sFilter = rs.getString("filter");
				tc.nTags = (asTags.length - 2) / 2;
				tc.atf = new TermFrequency[tc.nTags];
				for (int nTag = 0; nTag < tc.nTags; nTag++) {
					tc.atf[nTag] = new TermFrequency(asTags[(nTag+1)*2]);
				}
				vClouds.add(tc);
			}
			rs.close();
			ps.close();
			cn.close();
			return vClouds;
		} 
		catch(Exception e)
		{
			logger.error("getClouds error: ", e);
			throw e;
		}
	}
	public TagCloud getTopLevelCloud(String sGrainSize) throws Exception
	{
		try {
			Vector<TagCloud> vClouds = getClouds(LEVEL1_CLOUD, sGrainSize);
			return vClouds.size() > 0 ? vClouds.firstElement() : new TagCloud();
		} catch (Exception e) {
			logger.error("getTopLevelCloud error: ", e);
			throw e;
		}
		
	}
	
	private static void updateGrainSizeLanguageClouds(int nLanguageID, String sGrainSize, int nLevel) throws Exception 
	{
		try {
		switch (nLevel)
		{
		case 0:
		{
			final int TOP_LEVEL_TAG_COUNT = 200;
			new TagCloud(nLanguageID, TOP_LEVEL_TAG_COUNT).updateCloud(sGrainSize);
			break;
		}
		case 1:
		{
			final int SECOND_LEVEL_TAG_COUNT = 50;
			TagCloud tc = new TagCloud(nLanguageID).getTopLevelCloud(sGrainSize);
			for (int nTag = 0; nTag < tc.nTags; nTag++) 
			{
				new TagCloud(nLanguageID, SECOND_LEVEL_TAG_COUNT,tc.atf[nTag].sTerm).updateCloud(sGrainSize);
			}
			break;
		}
		case 2:
		{
			Vector<TagCloud> vtc = new TagCloud(nLanguageID).getClouds(LEVEL2_CLOUDS, sGrainSize);
			final int THIRD_LEVEL_TAG_COUNT = 50;
			for (Enumeration<TagCloud> eClouds = vtc.elements(); eClouds.hasMoreElements();)
			{
				TagCloud tc = eClouds.nextElement();
				for (int nTag = 0; nTag < tc.nTags; nTag++) 
				{
					new TagCloud(nLanguageID, THIRD_LEVEL_TAG_COUNT, getFilter(tc.sFilter, tc.atf[nTag].sTerm)).updateCloud(sGrainSize);
				}
			}
			break;
		}
		}
		} catch (Exception e) {
			logger.error("updateGrainSizeLanguageClouds error: ", e);
			throw e;
		}
	}

	public static void update(int nLevel) throws Exception 
	{
		logger.info("==========================================================Generate Tag Clouds - Level: " + nLevel);
		logger.info("Updating tag clouds - begin");
		for (Enumeration<Integer> eLanguageIDs = Locales.getLocaleIDs(); eLanguageIDs.hasMoreElements();) 
		{
			int nLanguageID = eLanguageIDs.nextElement();
			TagCloud.updateLanguageClouds(nLanguageID, nLevel);
		}
		logger.info("Updating tag clouds - end");
	}
	public static void update() throws Exception
	{
		for (int nLevel = 0; nLevel < 3; nLevel++) {
			update(nLevel);
		}
	}

	public static void main(String[] args) 
	{
		try 
		{
			getLoggerAndDBOptions("recommenderd.properties");
			if (args.length > 0) {
				int nLevel = 1;
				try{nLevel = Integer.parseInt(args[0]);}catch(Exception nfe){}
				update(nLevel);
			}
			else update();
		} catch (Exception e) {
			logger.error(e);
		}
	}
}
