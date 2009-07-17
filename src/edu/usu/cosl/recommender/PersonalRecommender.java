package edu.usu.cosl.recommender;

import java.sql.Statement;

import edu.usu.cosl.recommenderd.Base;
import edu.usu.cosl.util.Logger;

public class PersonalRecommender extends Base
{
	private void updatePersonalRecommendations() throws Exception
	{
		Logger.info("updatePersonalRecommendations-begin");
		
		cn = getConnection();
		
		Statement stTruncPersonalRec=cn.createStatement();
		stTruncPersonalRec.executeQuery("TRUNCATE TABLE personal_recommendations");
		stTruncPersonalRec.close();
		
		//Join table attentions, action_type, recommendations to generate table personal_recommendations
		Statement stAddPersonalRec=cn.createStatement();
		String sUpdateSQL =
				"INSERT INTO personal_recommendations (personal_recommendable_id,personal_recommendable_type,destination_id,relevance)" +
				"SELECT at.attentionable_id, at.attentionable_type,r.dest_entry_id,"+
				"MAX((at.weight)*(ac.weight)*(r.relevance)) as score "+
				"FROM attentions at, action_types ac, recommendations r "+
				"WHERE at.action_type=ac.action_type AND at.entry_id=r.entry_id "+
				"GROUP BY dest_entry_id , attentionable_id "+
				"ORDER BY attentionable_id";
		stAddPersonalRec.executeUpdate(sUpdateSQL);
		stAddPersonalRec.close();
		
		cn.close();
		
		Logger.info("updatePersonalRecommendations-end");
	}
	
	public static void update() throws Exception
	{
		new PersonalRecommender().updatePersonalRecommendations();
	}
	
	public static void main(String[] args) 
	{
		try {
			getLoggerAndDBOptions("recommenderd.properties");
			update();
		} catch (Exception e) {
			Logger.error(e);
		}
	}

}
