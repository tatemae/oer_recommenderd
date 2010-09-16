package edu.usu.cosl.recommenderd;

import java.sql.ResultSet;

import edu.usu.cosl.subjects.Subject;

import java.lang.Comparable;
import java.util.Vector;

public class EntryInfo implements Comparable<EntryInfo>
{
	public int nAggregationID;
	public int nFeedID;
	public int nEntryID;

	public int nLocalAggregationID;
	public int nLocalFeedID;
	public String sFeedURI;
	public String sFeedDisplayURI;
	public String sFeedTitle;
	public String sFeedShortTitle;
	public String sHarvestedFromTitle;
	public String sHarvestedFromShortTitle;
	public String sAuthor;
	public int nLanguageID;
	public int nLocalDocumentID;
	public int nRecommendationID;
	public boolean bDeleted;
	
	public String sTitle;
	public String sURI;
	public String sDirectLink;
	public String sDescription;
	public double dRelevance;
	public int nClicks;
	public long lAvgTimeAtDest = 60;
	public Vector<Subject> vSubjects;
	public Vector<Integer> vAggregations;
	
	public long lLastModified;
	
	public String sGrainSize; 
	
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
		sGrainSize = rsEntries.getString("grain_size");
	}
	public int compareTo(EntryInfo e){return this.lAvgTimeAtDest > e.lAvgTimeAtDest ? 1 : this.lAvgTimeAtDest == e.lAvgTimeAtDest ? 0 : -1;}

}

