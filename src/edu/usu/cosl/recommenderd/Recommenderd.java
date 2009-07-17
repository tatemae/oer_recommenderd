package edu.usu.cosl.recommenderd;

import edu.usu.cosl.aggregatord.Harvester;
import edu.usu.cosl.indexer.Indexer;
import edu.usu.cosl.recommender.PersonalRecommender;
import edu.usu.cosl.recommender.Recommender;
import edu.usu.cosl.tagclouds.TagCloud;
import edu.usu.cosl.tagclouds.SubjectAutoGenerator;
import edu.usu.cosl.util.Logger;

public class Recommenderd extends Base
{
	public Recommenderd(String sPropertiesFile, String sAction)
	{
		loadOptions(sPropertiesFile);
		if (sAction.equals("skip_harvest")) bHarvest = false;
		if (sAction.equals("rebuild")) {
			bHarvest = false;
			bReIndexAll = true;
			bRedoAllRecommendations = true;
		}
	}
	
	private void processDocuments()
	{
		Logger.status("processDocuments - begin");
		
		// use the aggregator to get any new records
		boolean bChanges = true;
		if (bHarvest) bChanges = Harvester.harvest();
		
		try
		{
			if (bChanges || bReIndexAll) 
			{
				// update the index
				Indexer.update(bReIndexAll);
				
				// for subjects that have fewer than 5 subjects (tags), we autogenerate some more
				SubjectAutoGenerator.update();
				
				// update to level tag clouds
				TagCloud.update();
				
				// seed the queries with common subjects
//				QueryUpdater.update();
			}
			if (bChanges || bRedoAllRecommendations) 
			{
				// create recommendations just for the new records, or for all
				Recommender.update(bRedoAllRecommendations);
				
				// create recommendations for new users or update recommendations for old ones
				PersonalRecommender.update();
			}
		}
		catch(Exception e){Logger.error(e);}

		Logger.status("processDocuments - end");

		notifyAdminOfResults();
	}
	
	public static void update(String sPropertiesFile, String sAction)
	{
		new Recommenderd(sPropertiesFile, sAction).processDocuments();
		Logger.stopLogging();
	}
	
	public static void main(String[] args) 
	{
		update("recommenderd.properties", args.length > 0 ? args[0] : "");
		Logger.stopLogging();
	}
}
