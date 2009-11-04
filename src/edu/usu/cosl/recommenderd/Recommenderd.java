package edu.usu.cosl.recommenderd;

import java.io.IOException;

import edu.usu.cosl.aggregatord.Harvester;
import edu.usu.cosl.indexer.Indexer;
import edu.usu.cosl.recommender.PersonalRecommender;
import edu.usu.cosl.recommender.Recommender;
import edu.usu.cosl.tagclouds.SubjectAutoGenerator;
import edu.usu.cosl.tagclouds.TagCloud;

public class Recommenderd extends Base {
	
	public Recommenderd(String sPropertiesFile, String sAction) throws IOException {
		loadOptions(sPropertiesFile);
		if (sAction.equals("skip_harvest"))
			bHarvest = false;
		if (sAction.equals("rebuild")) {
			bHarvest = false;
			bReIndexAll = true;
			bRedoAllRecommendations = true;
		}
	}

	private void processDocuments(String sPropertiesFile) throws IOException {
		logger.debug("processDocuments - begin");

		// use the aggregator to get any new records
		boolean bChanges = true;
		if (bHarvest)
			bChanges = Harvester.harvest(sPropertiesFile);

		if (isShutdownRequested()) return;
		
		try {
			if (bChanges || bReIndexAll) {

				// update the index
				Indexer.update(bReIndexAll);

				if (isShutdownRequested()) return;

				// for entries with fewer than 5 subjects (tags), we auto-generate more
				SubjectAutoGenerator.update();

				if (isShutdownRequested()) return;

				// update to level tag clouds
				TagCloud.update();

				// seed the queries with common subjects
				// QueryUpdater.update();
			}
			if (isShutdownRequested()) return;

			if (bChanges || bRedoAllRecommendations) {
				// create recommendations just for the new records, or for all
				Recommender.update(bRedoAllRecommendations);

				if (isShutdownRequested()) return;

				// create recommendations for new users or update
				// recommendations for old ones
				PersonalRecommender.update();
			}
		} catch (Exception e) {
			logger.error(e);
		}

		logger.debug("processDocuments - end");

		if (isShutdownRequested()) return;

		notifyAdminOfResults();
	}
	
	public static void main(String[] args) {
		System.out.println(" INFO [main] (Recommender daemon starting up");
		if (startup()) {
			try {
				String sPropertiesFile = "recommenderd.properties";
				String sAction = args.length > 0 ? args[0] : "";
				Recommenderd daemon = new Recommenderd(sPropertiesFile, sAction);

				while (!isShutdownRequested()) {
					daemon.processDocuments(sPropertiesFile);
					if(!isShutdownRequested())
						try{Thread.sleep(60*1000);}catch(InterruptedException e){}
				}
			}catch (IOException e) {}
		}
	}
}
