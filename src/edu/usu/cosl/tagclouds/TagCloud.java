package edu.usu.cosl.tagclouds;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import java.util.Arrays;
import java.util.Enumeration;

import edu.usu.cosl.recommenderd.Base;
import edu.usu.cosl.util.Locales;
import edu.usu.cosl.util.Logger;

public class TagCloud extends Base 
{
	int nUpdatedClouds = 0;

	PreparedStatement pstAddTagCloud;
	PreparedStatement pstGetTagCloud;

	int nTags;
	double dMin;
	double dMax;
	TermFrequency[] atf;
	int nLanguageID;
	int nMaxTags;
	String sFilter;
	String sGrainSize = "unknown";

	public TagCloud(int nLanguageID, int nMaxTags) {
		this(nLanguageID, nMaxTags, null);
	}

	public TagCloud(int nLanguageID, int nMaxTags, String sFilter) {
		this.nLanguageID = nLanguageID;
		this.nMaxTags = nMaxTags;
		this.atf = new TermFrequency[nMaxTags];
		this.sFilter = sFilter;
	}

	private void getTagCloud(PreparedStatement pstGetTags) throws SQLException {
		nTags = 0;
		dMin = 100000;
		dMax = 0;
		ResultSet rsTags = pstGetTags.executeQuery();
		while (rsTags.next()) {
			int nFreq = rsTags.getInt(2);
			if (nFreq < dMin)
				dMin = nFreq;
			if (nFreq > dMax)
				dMax = nFreq;
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

	private String getTagCloudString() {
		StringBuffer sbList = new StringBuffer(2500);

		// scale their frequencies and generate a list
		double dTagRange = dMax - dMin;
		final double dStyleRange = 5;
		for (int nTag = 0; nTag < nTags; nTag++) {
			TermFrequency tf = atf[nTag];
			if (nTag > 0)
				sbList.append(",");
			sbList.append(tf.sTerm);
			sbList.append(",");
			sbList.append(Math.round(((double) tf.nFrequency - dMin) * dStyleRange / dTagRange));
		}
		return sbList.toString();
	}

	private void storeTagCloud(String sTagCloud) throws SQLException {
		if (sTagCloud.length() > 0) {
			pstAddTagCloud.setString(1, sGrainSize);
			pstAddTagCloud.setInt(2, nLanguageID);
			pstAddTagCloud.setString(3, sFilter == null ? "" : sFilter);
			pstAddTagCloud.setString(4, sTagCloud);
//			Logger.info("" + sTagCloud.length() + " - " + sTagCloud);
			pstAddTagCloud.addBatch();
		}
		if (nUpdatedClouds % 100 == 0) {
			pstAddTagCloud.executeBatch();
			nUpdatedClouds = 0;
		}
	}

	private PreparedStatement getMultiTagStatement() throws SQLException {
		String[] asTags = sFilter.split("/");

		StringBuffer sb = new StringBuffer(
				"SELECT subjects.name, COUNT(subjects.id) as count "
						+ "FROM entries "
						+ "INNER JOIN entries_subjects ON entries_subjects.entry_id = entries.id "
						+ "INNER JOIN subjects ON entries_subjects.subject_id = subjects.id "
						+ "WHERE ");
		if ("course".equals(sGrainSize))
			sb.append("entries.grain_size = 'course' AND ");
		sb.append("entries.language_id = " + nLanguageID + " AND (");

		if (asTags.length > 0) {
			sb.append("entries.id IN ");
			sb.append("(");

			// The following code finds entries that share the same tags
			sb.append("SELECT et1.entry_id ");
			sb.append("FROM entries_subjects AS et1 ");
			sb.append("INNER JOIN subjects t1 ON t1.id = et1.subject_id ");

			for (int nTag = 2; nTag < asTags.length + 2; nTag++) {
				sb.append("INNER JOIN entries_subjects AS et" + nTag + " ON et"
						+ nTag + ".entry_id = et1.entry_id ");
				sb.append("INNER JOIN subjects t" + nTag + " ON t" + nTag
						+ ".id = et" + nTag + ".subject_id ");
			}
			sb.append("WHERE ");

			String connector = "";
			for (int nTag = 2; nTag < asTags.length + 2; nTag++) {
				sb.append(connector + " t" + nTag + ".name = ? ");
				connector = " AND ";
			}
			sb.append(") ) ");
		}
		sb.append("GROUP BY subjects.id, subjects.name ");
		sb.append("ORDER BY subjects.name ");
		sb.append("LIMIT ?");
		String sSql = sb.toString();
//		 Logger.info(sSql);
		PreparedStatement ps = cn.prepareStatement(sSql);
		for (int nTag = 0; nTag < asTags.length; nTag++) {
			ps.setString(nTag + 1, asTags[nTag]);
		}
		ps.setInt(asTags.length + 1, nMaxTags);
		return ps;
	}

	
	final static String sTagCloudSQL = "SELECT s.name, count(*) AS count "
			+ "FROM entries_subjects AS es "
			+ "INNER JOIN subjects AS s ON es.subject_id = s.id "
			+ "INNER JOIN entries AS e ON es.entry_id = e.id " + ""
			+ "WHERE e.language_id = ? " + "GROUP BY es.subject_id "
			+ "ORDER BY count DESC " + "LIMIT ?";
	final static String sGrainSizeTagCloudSQL = "SELECT s.name, count(*) AS count "
			+ "FROM entries_subjects AS es "
			+ "INNER JOIN subjects AS s ON es.subject_id = s.id "
			+ "INNER JOIN entries AS e ON es.entry_id = e.id "
			+ "WHERE e.grain_size = ? AND e.language_id = ? "
			+ "GROUP BY es.subject_id " + "ORDER BY count DESC " + "LIMIT ?";
	final static String sFilteredTagCloudSQL = "SELECT s2.name, count(*) AS count "
			+ "FROM subjects AS s1 "
			+ "INNER JOIN entries_subjects AS es ON s1.id = es.subject_id "
			+ "INNER JOIN entries AS e ON es.entry_id = e.id "
			+ "INNER JOIN entries_subjects AS es2 ON e.id = es2.entry_id "
			+ "INNER JOIN subjects AS s2 ON es2.subject_id = s2.id "
			+ "WHERE s1.name = ? AND e.language_id = ? "
			+ "GROUP BY es2.subject_id " + "ORDER BY count DESC LIMIT ?";
	final static String sGrainSizeFilteredTagCloudSQL = "SELECT s2.name, count(*) AS count "
			+ "FROM subjects AS s1 "
			+ "INNER JOIN entries_subjects AS es ON s1.id = es.subject_id "
			+ "INNER JOIN entries AS e ON es.entry_id = e.id "
			+ "INNER JOIN entries_subjects AS es2 ON e.id = es2.entry_id "
			+ "INNER JOIN subjects AS s2 ON es2.subject_id = s2.id "
			+ "WHERE e.grain_size = ? AND s1.name = ? AND e.language_id = ? "
			+ "GROUP BY es2.subject_id " + "ORDER BY count DESC LIMIT ?";

	private TagCloud updateCloud(String sGrainSize) throws Exception
	{
		try {
			
		cn = getConnection();
		pstAddTagCloud = cn.prepareStatement("REPLACE INTO tag_clouds SET grain_size = ?, language_id = ?, filter = ?, tag_list = ?");

		int nField = 1;
//		Logger.info(sFilter + "," + sGrainSize);
		if (sFilter != null && sFilter.contains("/")) 
		{
			pstGetTagCloud = getMultiTagStatement();
		}
		else 
		{
			if (sFilter == null) {
				if ("course".equals(sGrainSize)) {
					pstGetTagCloud = cn.prepareStatement(sGrainSizeTagCloudSQL);
					pstGetTagCloud.setString(nField++,sGrainSize);
				} else pstGetTagCloud = cn.prepareStatement(sTagCloudSQL);
			} else {
				if ("course".equals(sGrainSize)) {
					pstGetTagCloud = cn.prepareStatement(sGrainSizeFilteredTagCloudSQL);
					pstGetTagCloud.setString(nField++,sGrainSize);
				} else pstGetTagCloud = cn.prepareStatement(sFilteredTagCloudSQL);
				pstGetTagCloud.setString(nField++,sFilter);
			}
			pstGetTagCloud.setInt(nField++,nLanguageID);
			pstGetTagCloud.setInt(nField,nMaxTags);
		}
		getTagCloud(pstGetTagCloud);
		storeTagCloud(getTagCloudString());

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
		Logger.error(e);
		throw e;
		}
	}

	private static String getFilter(String sTag1, String sTag2) {
		if (sTag1.compareTo(sTag2) < 0)
			return sTag1 + "/" + sTag2;
		else
			return sTag2 + "/" + sTag1;
	}

	private static void updateLanguageClouds(int nLanguageID) throws Exception {
		final int TOP_LEVEL_TAG_COUNT = 200;
		TagCloud tc1 = new TagCloud(nLanguageID, TOP_LEVEL_TAG_COUNT).updateCloud("unknown");
		tc1.updateCloud("course");

		final int SECOND_LEVEL_TAG_COUNT = 100;
		for (int nTag = 0; nTag < tc1.nTags; nTag++) 
		{
			TagCloud tc2 = new TagCloud(nLanguageID, SECOND_LEVEL_TAG_COUNT,tc1.atf[nTag].sTerm).updateCloud("unknown");
			tc2.updateCloud("course");
			final int THIRD_LEVEL_TAG_COUNT = 100;
			for (int nTag2 = 0; nTag2 < tc2.nTags; nTag2++) 
			{
				TagCloud tc3 = new TagCloud(nLanguageID, THIRD_LEVEL_TAG_COUNT, getFilter(tc1.atf[nTag].sTerm, tc2.atf[nTag2].sTerm)).updateCloud("unknown");
				tc3.updateCloud("course");
			}
		}
	}

	public static void update() throws Exception 
	{
		Logger.status("Updating tag clouds - begin");
		for (Enumeration<Integer> eLanguageIDs = Locales.getLocaleIDs(); eLanguageIDs.hasMoreElements();) 
		{
			int nLanguageID = eLanguageIDs.nextElement();
			Logger.info("Generating tag cloud for language: " + Locales.getCode(nLanguageID));
			TagCloud.updateLanguageClouds(nLanguageID);
		}
		Logger.status("Updating tag clouds - end");
	}

	public static void main(String[] args) 
	{
		try 
		{
			getLoggerAndDBOptions("recommenderd.properties");
			update();
		} catch (Exception e) {
			Logger.error(e);
		}
	}
}
