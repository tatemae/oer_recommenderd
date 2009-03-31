package edu.usu.cosl.util;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.Timestamp;
import java.util.Date;

import edu.usu.cosl.recommenderd.Recommender;

public class DBThread extends Thread {

	protected boolean bStop = false;
	protected static Logger log = Recommender.getLogger();
	private final static String sDriver = "jdbc:apache:commons:dbcp:/";
	private static boolean bDriverLoaded = false;

	public static void loadDBDriver() throws ClassNotFoundException
	{
		// initialize the connection pool
//		Class.forName("org.postgresql.Driver");
		Class.forName("com.mysql.jdbc.Driver");
		Class.forName("org.apache.commons.dbcp.PoolingDriver");
		bDriverLoaded = true;
	}
	public static Connection getConnection(String sDBName) throws ClassNotFoundException, SQLException
	{
		if (!bDriverLoaded) loadDBDriver();
		return DriverManager.getConnection(sDriver + sDBName);
	}
	public void requestStop()
	{
		bStop = true;
	}
	public static Timestamp currentTime()
	{
		return new Timestamp(new Date().getTime());
	}
	public static Logger getLogger()
	{
		return log;
	}
}
