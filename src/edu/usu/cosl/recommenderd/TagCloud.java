package edu.usu.cosl.recommenderd;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Hashtable;

import edu.usu.cosl.util.Logger;
import edu.usu.cosl.util.DBThread;


public class TagCloud extends DBThread
{
	public static void updateClouds(Connection cnRecommender, Hashtable htIDToLanguage) throws SQLException
	{
		TagList.setupPreparedStatements(cnRecommender);
		for (Enumeration<Integer>eLanguageIDs = htIDToLanguage.keys(); eLanguageIDs.hasMoreElements();)
		{
			int nLanguageID = eLanguageIDs.nextElement();
			Logger.info("Generating tag cloud for language: " + htIDToLanguage.get(nLanguageID));
			TagList.updateLanguageClouds(nLanguageID);
		}
		TagList.closePreparedStatements();
	}
}
