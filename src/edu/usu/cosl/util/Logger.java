package edu.usu.cosl.util;

import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;

import java.text.SimpleDateFormat;

import java.io.FileWriter;
import java.io.PrintStream;
import java.io.OutputStream;
import java.sql.SQLException;

public class Logger extends OutputStream
{
	public static final int NEVER		= 0;
	public static final int CRITICAL	= 1;
	public static final int EXCEPTION	= 2;
	public static final int STATUS		= 3;
	public static final int WARNING		= 4;
	public static final int INFO		= 5;
	public static final int ALL			= 10;

	public static final int CONSOLE		= 1;
	public static final int STRING		= 2;
	public static final int FILE		= 3;
	public static final int DATABASE	= 4;
	private static int nOutputTo = STRING;
	private static StringBuffer sMsgs = new StringBuffer();
	
	private static int nLogLevel = CRITICAL;
//	private int nDBLogLevel = NEVER;
	private static boolean bLogToConsole = false;
	private static SimpleDateFormat timeFormatter = new SimpleDateFormat("hh:mm:ss a");
	private static SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");
	
	private static String sLogFilePrefix = null;
	private static FileLogger fileLogger;
	private static Logger logger;
	private static PrintStream out;
	private static PrintStream outConsole = System.out;
	private static PrintStream errConsole = System.err;
	
	private static ConcurrentLinkedQueue<String> messageQueue;

	public Logger ()
	{
//		out = new PrintStream(this);
//		System.setOut(out);
//		System.setErr(out);
	}
	public void write(int c)
	{
		outConsole.write(c);
	}
	public static void log(int nLevel, String sMessage)
	{
		if (nLevel <= nLogLevel) 
		{
			sMessage = timeFormatter.format(new Date()) + " " + sMessage;
			if (bLogToConsole) System.out.println(sMessage);
			if (messageQueue != null) messageQueue.add(sMessage);
			if (nOutputTo == STRING) sMsgs.append(sMessage + "\n");
		}
//		if (nLevel <= nDBLogLevel) logToDB(nLevel,nType,sMessage);
	}
	
	public static void fatal(String sMessage){log(CRITICAL,sMessage);}
	public static void error(String sMessage){log(EXCEPTION,sMessage);}
	public static void error(Exception e){log(e);}
	public static void error(Throwable t){log(t);}
	public static void warn(String sMessage){log(WARNING,sMessage);}
	public static void status(String sMessage){log(STATUS,sMessage);}
	public static void info(String sMessage){log(INFO,sMessage);}

	public static String getMessages()
	{
		return sMsgs.toString();
	}
	
	public static void setLogLevel(int nLevel)
	{
		nLogLevel = nLevel;
	}
	
	public static void setLogFilePrefix(String sPrefix)
	{
		sLogFilePrefix = sPrefix;
		if (sPrefix != null)
		{
			messageQueue = new ConcurrentLinkedQueue<String>();
			if (logger == null)
			{
				logger = new Logger();
				fileLogger = logger.new FileLogger();
				fileLogger.start();
			}
		}
		else
		{
			fileLogger.stopRunning();
			fileLogger = null;
			logger = null;
		}
	}

	public static void setLogToConsole(boolean bLog)
	{
		bLogToConsole = bLog;
		System.setOut(bLog ? outConsole : out);
		System.setErr(bLog ? errConsole : out);
	}

	public static void setDBLogLevel(int nLevel)
	{
//		nDBLogLevel = nLevel;
	}
	
	public static void logToDB(int nLevel, String sMessage)
	{
		// TODO: write code to log messages to a database
	}
	
	public static void log(Exception e)
	{
		if (e != null) log(EXCEPTION,e.toString());
		if (e instanceof SQLException) log(EXCEPTION, ((SQLException)e).getNextException().toString());
	}

	public static void error(String sMsg, Exception e)
	{
		if (e != null) log(EXCEPTION,sMsg + "\n" + e.toString());
		if (e instanceof SQLException) log(EXCEPTION, ((SQLException)e).getNextException().toString());
	}
	public static void log(Throwable t)
	{
		if (t != null) log(EXCEPTION,t.toString());
	}
	public static void getOptions(Properties properties)
	{
        // log level
        String sValue = properties.getProperty("debug_level");
        if (sValue != null) Logger.setLogLevel(Integer.parseInt(sValue));

        // log to console
        sValue = properties.getProperty("log_to_console");
        if ("true".equals(sValue)) Logger.setLogToConsole(true);

        // log to file
        sValue = properties.getProperty("log_file_prefix");
        if (sValue != null) Logger.setLogFilePrefix(sValue);
	}
	public class FileLogger extends Thread
	{
		private boolean bRun = true;
		
		public void run()
		{
			try 
			{
				while(bRun)
				{
					String sMessage = messageQueue.poll();
					if (sMessage != null) logMessageToFile(sMessage);
					else Thread.sleep((long)(1000));
				}
			}
			catch (InterruptedException e)
			{
				System.out.println(e);
			}
		}
		public void logMessageToFile(String sMessage)
		{
			try
			{
				FileWriter writer = new FileWriter(sLogFilePrefix + "_" + dateFormatter.format(new Date()) + ".log", true);
				writer.write(sMessage + "\r\n");
				writer.close();
			}
			catch (Exception e)
			{
				System.out.println(e);
			}
		}
		public void stopRunning()
		{
			bRun = false;
		}
	}
}
