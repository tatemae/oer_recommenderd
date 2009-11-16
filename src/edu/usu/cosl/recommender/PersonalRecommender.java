package edu.usu.cosl.recommender;

import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.ResultSet;

import java.util.Enumeration;
import java.util.Vector;

import edu.usu.cosl.recommenderd.Base;

public class PersonalRecommender extends Base
{
	private static final int MAX_PERSONAL_RECS = 20;
	private static final int MIN_ATTENTION_FOR_PERSONAL_RECS = 5;
	private static final String SQL_USERS_NEEDING_RECS = 
		"SELECT u.user_id, u.recs_needed FROM " +
		"(SELECT pr.personal_recommendable_id user_id, (" +
		MAX_PERSONAL_RECS +
		" - count(*)) recs_needed " +
		"FROM personal_recommendations pr " +
		"WHERE pr.visited_at IS NULL" +
		"GROUP BY pr.personal_recommendable_id, pr.personal_recommendable_type = 'User') u " +
		"WHERE u.recs_needed > 0 AND u.user_id IN (SELECT id FROM (SELECT u.id, count(*) AS count FROM users AS u " +
		"INNER JOIN attentions AS a ON u.id = a.attentionable_id AND a.attentionable_type = 'User') AS ua WHERE ua.count > " +
		MIN_ATTENTION_FOR_PERSONAL_RECS +
		")";
	
	private class User {
		public int nUserID;
		public int nRecsNeeded;
		public User(int nUserID, int nRecsNeeded) {
			this.nUserID = nUserID;
			this.nRecsNeeded = nRecsNeeded;
		}
	}
	
	private Vector<Integer> getUsersNeedingUpdate() throws SQLException{
		Vector<Integer> vUserIDs = new Vector<Integer>();
		Statement st = cn.createStatement();
		
		// users who have at least 5 items of attention and who have fewer
		// than 5 personal recommendations that they haven't clicked
		ResultSet rs = st.executeQuery(SQL_USERS_NEEDING_RECS);
		while (rs.next()) {
			vUserIDs.add(rs.getInt(1), rs.getInt(2));
		}
		rs.close();
		st.close();
		return vUserIDs;
	}
	
	private static final int ATTENTION_WRITE = 1;
	private static final int ATTENTION_BOOKMARK = 2;
    private static final int ATTENTION_SEARCH = 3;
    private static final int ATTENTION_CLICK = 4;
    private static final int ATTENTION_SHARE = 5;
    private static final int ATTENTION_DISCUSS = 6;
    
    private static final int DEFAULT_ATTENTION_WEIGHT = 5;
	
   private static final String SQL_CREATE_ATTENTIONS_FOR_PERSONAL_ENTRIES = "INSERT INTO attentions (attentionable_id, attentionable_type, entry_id, attention_type_id, weight, created_at) SELECT idf.ownable_id, 'User', e.id, " + ATTENTION_WRITE + ", " + DEFAULT_ATTENTION_WEIGHT + ", e.published_at FROM entries AS e INNER JOIN identity_feeds AS idf ON e.feed_id = idf.feed_id WHERE NOT EXISTS(SELECT id FROM attentions AS a WHERE a.entry_id = e.id AND a.attentionable_type = 'User' AND a.attentionable_id = idf.ownable_id)";
    
	// create attention for entries users have written that are not already in the attention table
	private void createAttentionsForPersonalEntries() throws SQLException{
		Statement st = cn.createStatement();
		st.executeUpdate(SQL_CREATE_ATTENTIONS_FOR_PERSONAL_ENTRIES);
		st.close();
	}
	
	// rank entries by (attention_type_weight)(attention_recency)(frequency)(attention_weight)(relevance)(resource recency)
	private void createRecommendationsForUser(User u) throws SQLException{
		pstGetPersonalRecs.setInt(1, u.nUserID);
		pstGetPersonalRecs.setInt(2, u.nUserID);
		pstGetPersonalRecs.setInt(3, u.nRecsNeeded);
		ResultSet rsRecs = pstGetPersonalRecs.executeQuery();
		while (rsRecs.next()) {
			
		}
		rsRecs.close();
	}
	
	private PreparedStatement pstGetPersonalRecs;
	private static final String SQL_GET_USER_RECS = 
		"INSERT INTO personal_recommendations (personal_recommendable_id, personal_recommendable_type, destination_id, destination_type, created_at, relevance) " + 
		"(SELECT a.attentionable_id, 'User', r.dest_entry_id, 'Entry', now(), " +  
		"MAX((a.weight)*(at.default_weight)*(r.relevance)*( 10-9/(1+exp(10-datediff(now(), a.created_at))))*( 10-9/(1+exp(10-datediff(now(), e.published_at))))) as score " + 
		"FROM attentions a, attention_types at, recommendations r, entries e " +  
		"WHERE a.attention_type_id=at.id AND a.entry_id=r.entry_id AND e.id = r.entry_id " + 
		"AND (r.dest_entry_id NOT IN (SELECT pr.destination_id FROM personal_recommendations pr WHERE pr.destination_type = 'Entry' AND pr.personal_recommendable_id = ? AND pr.personal_recommendable_type = 'User')) " + 
		"AND a.attentionable_id = ? AND a.attentionable_type = 'User' " + 
		"GROUP BY r.dest_entry_id, a.attentionable_id " + 
		"ORDER BY score DESC " + 
		"LIMIT ?)";
	
	private void updatePersonalRecommendations(boolean bRedo) throws Exception
	{
		logger.debug("==========================================================Personal Recommendations - Begin");
		
		cn = getConnection();
		
		createAttentionsForPersonalEntries();
		
		pstGetPersonalRecs = cn.prepareStatement(SQL_GET_USER_RECS);
		
		for (Enumeration<Integer>eUserIDs = getUsersNeedingUpdate().elements(); eUserIDs.hasMoreElements();) {
			createRecommendationsForUser(eUserIDs.nextElement(), 20);
		}
		
		cn.close();
		
		logger.debug("==========================================================Personal Recommendations - End");
	}
	
	public static void update(boolean bRedo) throws Exception
	{
		new PersonalRecommender().updatePersonalRecommendations(bRedo);
	}
	
	public static void main(String[] args) 
	{
		try {
			getLoggerAndDBOptions("recommenderd.properties");
			update(args.length > 0 && args[0].equals("all"));
		} catch (Exception e) {
			logger.error(e);
		}
	}

}

