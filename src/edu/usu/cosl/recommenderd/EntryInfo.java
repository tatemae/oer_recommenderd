package edu.usu.cosl.recommenderd;

import java.sql.ResultSet;
import java.lang.Comparable;

public class EntryInfo implements Comparable<EntryInfo>
{
	int nAggregationID;
	int nFeedID;
	int nEntryID;

	int nLocalAggregationID;
	int nLocalFeedID;
	String sFeedURI;
	String sFeedDisplayURI;
	String sFeedTitle;
	String sFeedShortTitle;
	String sHarvestedFromTitle;
	String sHarvestedFromShortTitle;
	String sAuthor;
	String sLanguage;
	int nLocalDocumentID;
	int nRecommendationID;
	
	String sTitle;
	String sURI;
	String sDirectLink;
	String sDescription;
	String sTagList;
	double dRelevance;
	int nClicks;
	long lAvgTimeAtDest = 60;
	
	long lLastModified;
	
	public EntryInfo(){}
	public EntryInfo(ResultSet rsEntries) throws Exception
	{
		nFeedID = rsEntries.getInt("feed_id");
		nEntryID = rsEntries.getInt("id");
		sURI = rsEntries.getString("permalink");
		sDirectLink = rsEntries.getString("direct_link");
		sTitle = rsEntries.getString("title");
		sDescription = rsEntries.getString("description");
		sTagList = rsEntries.getString("tag_list");
		sLanguage = rsEntries.getString("language");
		if (sLanguage == null || sLanguage.length() < 2) sLanguage = "en";
		else sLanguage = sLanguage.toLowerCase().substring(0, 2);
	}
	public int compareTo(EntryInfo e){return this.lAvgTimeAtDest > e.lAvgTimeAtDest ? 1 : this.lAvgTimeAtDest == e.lAvgTimeAtDest ? 0 : -1;}
}

