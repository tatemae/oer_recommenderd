package edu.usu.cosl.recommenderd;

import java.io.IOException;
import java.util.GregorianCalendar;
import java.util.Calendar;

import edu.usu.cosl.aggregatord.Harvester;
import edu.usu.cosl.indexer.Indexer;
import edu.usu.cosl.recommender.PersonalRecommender;
import edu.usu.cosl.recommender.Recommender;
import edu.usu.cosl.subjects.SubjectAutoGenerator;
import edu.usu.cosl.tagclouds.TagCloud;

public class Recommenderd extends Base {
	
	private static int nSecondsToSleepBetweenTasks = 60;
	
	private String sPropertiesFile;
	private String sTask;
	private boolean bFull = false;
	private boolean bFullUpdateRanToday = false;
	
	public Recommenderd(String sPropertiesFile, String sTask, boolean bFull) throws IOException {
		this.sPropertiesFile = sPropertiesFile;
		this.sTask = sTask;
		this.bFull = bFull;
		loadOptions(sPropertiesFile);
	}
	
	private boolean setFull(boolean bFull) {
		boolean bOldFull = this.bFull;
		this.bFull = bFull;
		return bOldFull;
	}

	private boolean harvest() throws IOException{return Harvester.harvest(sPropertiesFile);}
	private boolean harvest(int nMaxNewEntries) throws IOException{return Harvester.harvest(sPropertiesFile,nMaxNewEntries);}
	private void index() throws Exception{Indexer.update(bFull);}
	private void recommend() throws Exception{Recommender.update(sPropertiesFile,bFull);}
	private void autoGenerateSubjects() throws Exception{SubjectAutoGenerator.update();}
	private void tagClouds() throws Exception{tagClouds(3);}
	private void tagClouds(int nDepth) throws Exception{TagCloud.update(nDepth);}
	private void personalRecommendations() throws Exception{PersonalRecommender.update(bFull);}

	private void bootstrap() throws Exception{
		bFull = false;
		final int NUM_BOOTSTRAP_ENTRIES = 200;
		if (harvest(NUM_BOOTSTRAP_ENTRIES)) {
			if (isShutdownRequested()) return;
			index();
			if (isShutdownRequested()) return;
			recommend();
			if (isShutdownRequested()) return;
			autoGenerateSubjects();
			if (isShutdownRequested()) return;
			tagClouds(1);
		}
	}

	private boolean isFullUpdateDay(){
		return nFullUpdateDay == new GregorianCalendar().get(Calendar.DAY_OF_WEEK);
	}
	
	private boolean isFullUpdateHour(){
		return nFullUpdateHour == new GregorianCalendar().get(Calendar.HOUR_OF_DAY);
	}
	
	private boolean timeForFullUpdate() {
		if (isFullUpdateDay()) {
			if (!bFullUpdateRanToday && isFullUpdateHour()) return true;
		} else {
			bFullUpdateRanToday = false;
		}
		return false;
	}
	
	private void doTask() throws Exception {
		if ("daemon".equals(sTask)) {
			if (bFull || timeForFullUpdate()) fullUpdate();
			else incrementalUpdate();
		}
		else if ("harvest".equals(sTask)) harvest();
		else if ("index".equals(sTask)) index();
		else if ("recommend".equals(sTask)) recommend();
		else if ("subjects".equals(sTask)) autoGenerateSubjects();
		else if ("tag_clouds".equals(sTask)) tagClouds();
		else if ("personal_recommendations".equals(sTask)) personalRecommendations();
		else if ("bootstrap".equals(sTask)) bootstrap();
	}

	private void incrementalUpdate() throws Exception {
		boolean bOldFull = setFull(false);
		if (harvest()) {
			if (isShutdownRequested()) return;
			index();
			if (isShutdownRequested()) return;
			recommend();
			if (isShutdownRequested()) return;
			personalRecommendations();
			if (isShutdownRequested()) return;
			notifyAdminOfResults();
		}
		setFull(bOldFull);
	}
	
	private void fullUpdate() throws Exception {
		boolean bOldFull = setFull(true);
		if (harvest()) {
			if (isShutdownRequested()) return;
			index();
			if (isShutdownRequested()) return;
			recommend();
			if (isShutdownRequested()) return;
			autoGenerateSubjects();
			if (isShutdownRequested()) return;
			index();
			if (isShutdownRequested()) return;
			tagClouds();
			if (isShutdownRequested()) return;
			personalRecommendations();
			if (isShutdownRequested()) return;
			notifyAdminOfResults();
		}
		setFull(bOldFull);
		bFullUpdateRanToday = true;
	}
	
	public static void main(String[] args) {
		System.out.println(" INFO [main] Recommender daemon starting up");
		if (startup()) {
			try {
				String sPropertiesFile = "recommenderd.properties";
				String sTask = args.length > 0 ? args[0] : "daemon";
				boolean bFull = args.length > 1 && "redo".equals(args[1]); 
				Recommenderd daemon = new Recommenderd(sPropertiesFile, sTask, bFull);
				
				if ("daemon".equals(sTask)) {
	
					while (!isShutdownRequested()) {
						daemon.doTask();
	
						if(!isShutdownRequested()) {
							try{Thread.sleep(nSecondsToSleepBetweenTasks*1000);}catch(InterruptedException e){}
						}
					}
				} else {
					daemon.doTask();
				}
			}catch (Exception e) {System.out.println(e);}
		}
	}
}
