package edu.usu.cosl.recommenderd;

import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.util.Hashtable;
import java.util.Enumeration;

import edu.usu.cosl.util.DBThread;
import edu.usu.cosl.util.Logger;

public class Locales extends DBThread {
	private static Hashtable<Integer, String> htIDToLanguage;
	private static Hashtable<String, Integer> htLanguageToID;

	private static void loadMappings()
	{
		if (htIDToLanguage != null) return;
		htIDToLanguage = new Hashtable<Integer, String>();
		htLanguageToID = new Hashtable<String, Integer>();
    	try
    	{
    		Connection cn = getConnection();
	    	Statement st = cn.createStatement();
	    	ResultSet rs = st.executeQuery("SELECT id, locale, is_default FROM languages where muck_raker_supported = 1");
	    	while(rs.next()){
	    		Integer nLanguageID = rs.getInt(1);
	    		String sLocale = rs.getString(2).substring(0,2);
	    		htIDToLanguage.put(nLanguageID,sLocale);
	    		htLanguageToID.put(sLocale,nLanguageID);
	    	}
	    	rs.close();
	    	st.close();
	    	cn.close();
    	}
    	catch(Exception e)
    	{
    		Logger.error("Unable to read language mappings from db", e);
    	}
		
	}
	
	public static int getID(String sLocale)
	{
		loadMappings();
		return htLanguageToID.get(sLocale);
	}
	public static String getCode(int nLanguageID)
	{
		loadMappings();
		return htIDToLanguage.get(nLanguageID);
	}
	public static Enumeration<Integer> getLocaleIDs()
	{
		loadMappings();
		return htIDToLanguage.keys();
	}
}
