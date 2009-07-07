package edu.usu.cosl.recommenderd;

import edu.usu.cosl.aggregatord.Harvester;
import edu.usu.cosl.indexer.Indexer;
import edu.usu.cosl.tagclouds.TagCloud;
import edu.usu.cosl.tagclouds.SubjectAutoGenerator;
import edu.usu.cosl.util.Logger;

public class Recommenderd extends Base
{
	public Recommenderd(String sPropertiesFile, String sAction)
	{
		loadOptions(sPropertiesFile);
		if (sAction.equals("skip_harvest")) bHarvest = false;
	}
	
	private void processDocuments()
	{
		Logger.status("processDocuments - begin");
		
		// use the aggregator to get any new records
		boolean bChanges = true;
		if (bHarvest) bChanges = Harvester.harvest();
		
		try
		{
			// update the index
			if (bChanges || bReIndexAll) Indexer.update(bReIndexAll);
			
			if (bChanges) 
			{
				// for subjects that have fewer than 5 subjects (tags), we autogenerate some more
				SubjectAutoGenerator.update();
				
				// update to level tag clouds
				TagCloud.update(1);
				
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
			if (bChanges) {
				// update second and third level tag clouds
				for (int nLevel = 2; nLevel <= nTagCloudDepth && nLevel <= 3; nLevel++) {
					TagCloud.update(nLevel);
				}
			}
		}
		catch(Exception e){Logger.error(e);}

		Logger.status("processDocuments - end");

		notifyAdminOfResults();
		
		Logger.setLogFilePrefix(null);
	}
	
	public static void update(String sPropertiesFile, String sAction)
	{
		new Recommenderd(sPropertiesFile, sAction).processDocuments();
	}
	
	public static void main(String[] args) 
	{
		update("recommenderd.properties", args.length > 0 ? args[0] : "");
		Logger.stopLogging();
	}
}
