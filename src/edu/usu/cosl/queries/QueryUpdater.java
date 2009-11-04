package edu.usu.cosl.queries;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import edu.usu.cosl.recommenderd.Base;

public class QueryUpdater extends Base {

	private void updateQueries()
	{
		logger.info("updateQueries - begin");

		try
		{
			Statement stSubjectFreq=cn.createStatement();
			ResultSet subjectFreq=stSubjectFreq.executeQuery("select subject_id, count(*) from taggings group by subject_id");
			subjectFreq.last();
			int num=subjectFreq.getRow();
			subjectFreq.first();
			int []count=new int[num];
			int i=0,max=0;
			do
		    {
		    	count[i]=subjectFreq.getInt(2);
		    	if(max<=count[i])
		    		max=count[i];
		    	i++;
		    }while(subjectFreq.next());
			
			PreparedStatement insertIntoQueries=cn.prepareStatement("Insert Into queries(name,frequency)"+
				"values ((select name from subjects where subjects.id=?),?)");
			PreparedStatement searchSubsinQueries=cn.prepareStatement("SELECT queries.id,queries.frequency FROM queries, subjects "+
				"where subjects.name=queries.name and subjects.id=?");
			PreparedStatement updateIntoQueries=cn.prepareStatement("Update queries set frequency=? where id=?");
			for(i=0;i<num;i++)
			{
				count[i]=(int)((float)(count[i])/(float)(max)*100.0);
				searchSubsinQueries.setInt(1, i+1);
				ResultSet subsinQueries =searchSubsinQueries.executeQuery();
				if(subsinQueries.next())
				{
					if(subsinQueries.getInt(2)<100)
					{
						updateIntoQueries.setInt(1, count[i]);
						updateIntoQueries.setInt(2, subsinQueries.getInt(1));
						updateIntoQueries.addBatch();
					}
				}
				else
				{
					insertIntoQueries.setInt(1, i+1);
					insertIntoQueries.setInt(2, count[i]);
					insertIntoQueries.addBatch();
				}
			}
			insertIntoQueries.executeBatch();
			updateIntoQueries.executeBatch();
		}
		catch (Exception e)
		{
			logger.error("updateQueries - ", e);
		}
		logger.info("updateQueries - end");
	}

	public static void update() throws Exception
	{
		new QueryUpdater().updateQueries();
	}
	
	public static void main(String[] args) 
	{
		try {
			getLoggerAndDBOptions("recommenderd.properties");
			update();
		} catch (Exception e) {
			logger.error(e);
		}
	}

}
