package edu.usu.cosl.recommender.personal;

import java.io.File;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.ResultSet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Vector;
import java.util.Hashtable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.lucene.analysis.Analyzer;
import org.apache.mahout.clustering.canopy.CanopyDriver;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.distance.ManhattanDistanceMeasure;
import org.apache.mahout.common.distance.TanimotoDistanceMeasure;
import org.apache.mahout.utils.vectors.text.DictionaryVectorizer;
import org.apache.mahout.utils.vectors.text.DocumentProcessor;
import org.apache.mahout.utils.vectors.tfidf.TFIDFConverter;

import edu.usu.cosl.recommenderd.Base;

public class PersonalRecommender extends Base
{
	private class User {
		public int nUserID;
		public int nRecsNeeded;
		public User(int nUserID, int nRecsNeeded) {
			this.nUserID = nUserID;
			this.nRecsNeeded = nRecsNeeded;
		}
	}
	
//	private static final String SQL_USERS_NEEDING_RECS = 
//		"SELECT u.user_id, u.recs_needed FROM " +
//		"(SELECT pr.personal_recommendable_id user_id, (" +
//		MAX_PERSONAL_RECS +
//		" - count(*)) recs_needed " +
//		"FROM personal_recommendations pr " +
//		"WHERE pr.visited_at IS NULL" +
//		"GROUP BY pr.personal_recommendable_id, pr.personal_recommendable_type = 'User') u " +
//		"WHERE u.recs_needed > 0 AND u.user_id IN (SELECT id FROM (SELECT u.id, count(*) AS count FROM users AS u " +
//		"INNER JOIN attentions AS a ON u.id = a.attentionable_id AND a.attentionable_type = 'User') AS ua WHERE ua.count > " +
//		MIN_ATTENTION_FOR_PERSONAL_RECS +
//		")";

	// we create new recommendations for:
	// every day - for users who have logged in the past week
	// once a week - for users who have logged in the past month
	private static final String SQL_RECS_AGE = 
		"SELECT ur.id FROM " +
			"(SELECT u.id, max(datediff(now(), pr.created_at)) recs_days_old " +
			"FROM personal_recommendations pr INNER " +
			"JOIN users u ON pr.personal_recommendable_id = u.id " +
			"AND pr.personal_recommendable_type = 'User' " +
			"WHERE visited_at IS NULL GROUP BY pr.personal_recommendable_id) ur " +
		"WHERE ur.recs_days_old > ? ";

	private static final String SQL_DAYS_SINCE_LOGIN =
		"(SELECT u.id FROM users u WHERE datediff(now(),u.last_request_at) < ?) ";

	private static final String SQL_USERS_WITHOUT_RECOMMENDATIONS =
		"SELECT u2.id FROM users u2 WHERE NOT EXISTS " +
		"(SELECT * FROM personal_recommendations pr " +
		"WHERE pr.personal_recommendable_type = 'User' " +
		"AND pr.personal_recommendable_id = u2.id) ";

	private static final String SQL_USERS_WITH_ENOUGH_ATTENTION = 
		"(SELECT ua.id FROM " +
			"(SELECT u3.id, count(*) AS count FROM users u3 " +
			"INNER JOIN attentions AS a ON u3.id = a.attentionable_id " +
			"AND a.attentionable_type = 'User') AS ua " +
		"WHERE ua.count > ?) ";

	private static final String SQL_USERS_NEEDING_RECS = 
		SQL_RECS_AGE +
		"AND ur.id IN " +
		SQL_DAYS_SINCE_LOGIN +
		"UNION " +
		SQL_USERS_WITHOUT_RECOMMENDATIONS +
		"AND u2.id IN " +
		SQL_USERS_WITH_ENOUGH_ATTENTION;
	
	private Vector<User> getUsersNeedingUpdate(boolean bWeekly) throws SQLException{
		Vector<User> vUsers = new Vector<User>();
		vUsers.add(new User(235, 20));
//		logger.info(SQL_USERS_NEEDING_RECS);
//		PreparedStatement pst = cn.prepareStatement(SQL_USERS_NEEDING_RECS);
//		
//		// how long since they have logged in
//		final int DAYS_TO_WAIT_FOR_PEOPLE_WHO_HAVE_LOGGED_IN_DURING_THE_PAST_WEEK = 1;
//		final int DAYS_TO_WAIT_FOR_PEOPLE_WHO_HAVENT_LOGGED_IN_FOR_OVER_A_WEEK = 7;
//		pst.setInt(1, bWeekly ? DAYS_TO_WAIT_FOR_PEOPLE_WHO_HAVENT_LOGGED_IN_FOR_OVER_A_WEEK : DAYS_TO_WAIT_FOR_PEOPLE_WHO_HAVE_LOGGED_IN_DURING_THE_PAST_WEEK);
//
//		// how old their recommendations are
//		final int WEEK = 7;
//		final int MONTH = 30;
//		pst.setInt(2, bWeekly ? MONTH : WEEK);
//
//		// minimum number of attentions before we start giving personal recommendations
//		final int MIN_ATTENTION_FOR_PERSONAL_RECS = 5;
//		pst.setInt(3, MIN_ATTENTION_FOR_PERSONAL_RECS);
//
//		final int MAX_PERSONAL_RECS = 20;
//		ResultSet rs = pst.executeQuery();
//		while (rs.next()) {
//			vUsers.add(new User(rs.getInt(1), MAX_PERSONAL_RECS));
//		}
//		rs.close();
//		pst.close();
		return vUsers;
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
	
	private static final String SQL_DELETE_OLD_RECS = 
		"DELETE FROM personal_recommendations WHERE personal_recommendable_type = 'User' AND visited_at IS NULL AND personal_recommendable_id = ? ";
	
	private PreparedStatement pstDeleteOldRecs;
	
	private void deleteOldRecs(User u) throws SQLException{
		pstDeleteOldRecs.setInt(1, u.nUserID);
		pstDeleteOldRecs.executeUpdate();
	}
	
	class SimpleDoc implements Comparable<SimpleDoc>{
		int category;
		String sTitle;
		SimpleDoc(int category, String sTitle) {
			this.category = category;
			this.sTitle = sTitle;
		}
		public int compareTo(SimpleDoc o) {
			// TODO Auto-generated method stub
			return category - o.category;
		}
	}
	
	private void createRecommendationsForUser(User u) throws Exception{
		int minSupport = 2;
		int minDf = 5;
		int maxDFPercent = 80;
		int maxNGramSize = 2;
		int minLLRValue = 50;
		int reduceTasks = 1;
		int chunkSize = 200;
		int norm = 2;
		boolean sequentialAccessOutput = true;
		Hashtable<Integer,String> htDocs = new Hashtable<Integer,String>();

		// prepare a directory to store sequence files for the documents
		String inputDir = "inputDir";
		File inputDirFile = new File(inputDir);
		if (!inputDirFile.exists()) {
			inputDirFile.mkdir();
		}
		// load the hadoop file system configuration
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		// loop through documents creating sequence files
		SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, new Path(inputDir, "documents.seq"), Text.class, Text.class);
		pstGetAttentionRecs.setInt(1,u.nUserID);
		ResultSet rs = pstGetAttentionRecs.executeQuery();
		while (rs.next()) {
			htDocs.put(rs.getInt(1),rs.getString(2));
			writer.append(new Text(rs.getInt(1) + ""), new Text(rs.getString(2) + rs.getString(3)));
		}
		writer.close();

		// prepare a directory to store cluster data
		String outputDir = "newsClusters";
		HadoopUtil.overwriteOutput(outputDir);
		String tokenizedPath = outputDir + DocumentProcessor.TOKENIZED_DOCUMENT_OUTPUT_FOLDER;

		// tokenize the sequence files
//		Analyzer analyzer = htAnalyzers.get("en");
		Analyzer analyzer = new MyAnalyzer();
		DocumentProcessor.tokenizeDocuments(inputDir, analyzer.getClass().asSubclass(Analyzer.class), tokenizedPath);

		// create term frequency vectors
		DictionaryVectorizer.createTermFrequencyVectors(tokenizedPath, outputDir, minSupport, maxNGramSize, minLLRValue, reduceTasks, chunkSize, sequentialAccessOutput);
		TFIDFConverter.processTfIdf(outputDir + DictionaryVectorizer.DOCUMENT_VECTOR_OUTPUT_FOLDER, outputDir + TFIDFConverter.TFIDF_OUTPUT_FOLDER, chunkSize, minDf, maxDFPercent, norm, sequentialAccessOutput);

		String vectorsFolder = outputDir + TFIDFConverter.TFIDF_OUTPUT_FOLDER + "/vectors";
		String canopyCentroids = outputDir + "/canopy-centroids";
		String clusterOutput = outputDir + "/clusters";

		// do canopy clustering to quickly identify # of clusters
		CanopyDriver.runJob(vectorsFolder, canopyCentroids, MyDistanceMeasure.class.getName(), 70, 20);

		// do the kmeans clustering
		KMeansDriver.runJob(vectorsFolder, canopyCentroids, clusterOutput, TanimotoDistanceMeasure.class.getName(), 0.01, 10, 1);

		System.out.println("range of distances: " + MyDistanceMeasure.minDistance + ", " + MyDistanceMeasure.maxDistance + ", "  + MyDistanceMeasure.avgDistance + " =======================================================");

		// loop through the cluster files
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(clusterOutput + "/points/part-00000"), conf);
		Text key = new Text();
		Text value = new Text();
		ArrayList<SimpleDoc> docs = new ArrayList<SimpleDoc>();
		while (reader.next(key, value)) {
			docs.add(new SimpleDoc(Integer.parseInt(value.toString()), htDocs.get(Integer.parseInt(key.toString()))));
//			System.out.println(value.toString() + " - " + htDocs.get(Integer.parseInt(key.toString())));
			// Write code here to save the cluster mapping to the database
		}
		reader.close();
		Collections.sort(docs);
		for (int n = 0; n < docs.size(); n++) {
			SimpleDoc doc = docs.get(n);
			System.out.println(doc.category + " - " + doc.sTitle);
		}
	}
	
	// rank entries by (attention_type_weight)(attention_recency)(frequency)(attention_weight)(relevance)(resource recency)
	private void createRecommendationsForUserOld(User u) throws SQLException{
		deleteOldRecs(u);
		pstAddPersonalRecs.setInt(1, u.nUserID);
		pstAddPersonalRecs.setInt(2, u.nUserID);
		pstAddPersonalRecs.setInt(3, u.nRecsNeeded);
		pstAddPersonalRecs.executeUpdate();
	}
	
	private PreparedStatement pstAddPersonalRecs;
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
	
	private PreparedStatement pstGetAttentionRecs;
	private static final String SQL_GET_ATTENTION_RECS = 
		"select entries.id, entries.title, entries.description from entries inner join attentions on attentions.entry_id = entries.id where attentionable_id = ? and attentionable_type = 'User'";
//	"select entries.id, entries.title, entries.description from entries where entries.language_id = 38 limit 1000";
	
	private void updatePersonalRecommendations(boolean bFull) throws Exception
	{
		logger.debug("==========================================================Personal Recommendations - Begin");
		
		cn = getConnection();
		
		createAttentionsForPersonalEntries();
		
//		pstAddPersonalRecs = cn.prepareStatement(SQL_GET_USER_RECS);
//		pstDeleteOldRecs = cn.prepareStatement(SQL_DELETE_OLD_RECS);
		pstGetAttentionRecs = cn.prepareStatement(SQL_GET_ATTENTION_RECS);
		
//		createAnalyzers();

		for (Enumeration<User>eUsers = getUsersNeedingUpdate(bFull).elements(); eUsers.hasMoreElements();) {
			createRecommendationsForUser(eUsers.nextElement());
		}
		
//		closeCores();
		
		pstGetAttentionRecs.close();
//		pstDeleteOldRecs.close();
//		pstAddPersonalRecs.close();
		
		cn.close();
		
		logger.debug("==========================================================Personal Recommendations - End");
	}
	
	public static void update(boolean bFull) throws Exception
	{
		PersonalRecommender pr = new PersonalRecommender();
		pr.loadOptions("recommenderd.properties");
		pr.updatePersonalRecommendations(bFull);
	}
	
	public static void main(String[] args) 
	{
		try {
//			getLoggerAndDBOptions("recommenderd.properties");
			update(args.length > 0 && args[0].equals("all"));
		} catch (Exception e) {
			logger.error(e);
		}
	}

}

