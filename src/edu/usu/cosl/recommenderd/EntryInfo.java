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
    int nLanguageID;
	int nLocalDocumentID;
	int nRecommendationID;
	
	String sTitle;
	String sURI;
	String sDirectLink;
	String sDescription;
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
		nLanguageID = rsEntries.getInt("language_id");
	}
	public int compareTo(EntryInfo e){return this.lAvgTimeAtDest > e.lAvgTimeAtDest ? 1 : this.lAvgTimeAtDest == e.lAvgTimeAtDest ? 0 : -1;}
}

