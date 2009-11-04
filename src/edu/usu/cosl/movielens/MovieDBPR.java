package edu.usu.cosl.movielens;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.io.*;
import java.lang.Math;

import edu.usu.cosl.util.DBThread;
import edu.usu.cosl.util.Logger;

public class MovieDBPR extends DBThread
{
	private Connection cnRecommender;
	private double eps=0.000000001;
	
	public void updatePersonalRecommendations_SS() throws SQLException
	{
		Logger.info("updatePersonalRecommendations-begin");
		
		Statement stTruncPersonalRec=cnRecommender.createStatement();
		stTruncPersonalRec.executeQuery("TRUNCATE TABLE personal_recommendations");
		//Join table attentions, action_type, recommendations to generate table personal_recommendations
		PreparedStatement pstAddPersonalRec=cnRecommender.prepareStatement(
				"INSERT INTO personal_recommendations (personal_recommendable_id,personal_recommendable_type,destination_id,relevance)" +
				"SELECT at.attentionable_id, at.attentionable_type,r.dest_entry_id,"+
				"MAX((at.weight)*(ac.weight)*(r.relevance)) as score "+
				"FROM attentions at, action_types ac, recommendations r "+
				"WHERE at.action_type=ac.action_type AND at.entry_id=r.entry_id "+
				"GROUP BY dest_entry_id , attentionable_id "+
				"ORDER BY attentionable_id");
		pstAddPersonalRec.execute();
		pstAddPersonalRec.close();
		
		Logger.info("updatePersonalRecommendations-end");
	}
	public void updatePersonalRecommendations_CF() throws SQLException
	{
		Statement stUserNumber=cnRecommender.createStatement();
		Statement stEntryNumber=cnRecommender.createStatement();
		ResultSet rsUserNumber=stUserNumber.executeQuery("select count(*) from movie_users");
		ResultSet rsEntryNumber=stEntryNumber.executeQuery("select max(id) from movies");
		rsUserNumber.next();
		int num_User=rsUserNumber.getInt(1);
		rsEntryNumber.next();
		int num_Entry=rsEntryNumber.getInt(1);
		stUserNumber.close();
		stEntryNumber.close();
		
	

		
		double [][] a=new double [num_User][num_Entry];//attention matrix
		double [][] rcf=new double[num_User][num_Entry];//Collaborate score
		double [][] su=new double[num_User][num_User];//user similarity matrix
		Statement stAttention=cnRecommender.createStatement();
		ResultSet rsAttention=stAttention.executeQuery("SELECT user_id, movie_id, rate FROM movie_ratings");
		while(rsAttention.next())
		{
			int user_id=rsAttention.getInt(1);
			int movie_id=rsAttention.getInt(2);
			a[user_id-1][movie_id-1]=Math.max((double)(rsAttention.getInt(3)/5.0), a[user_id-1][movie_id-1]);//maybe multiple attentions 
		}
		//calculate similarity scores between users
		for(int i=0;i<num_User;i++)
			for(int j=0;j<num_User;j++)
			{
				double summ=0.0;
				double sumi=0.0;
				double sumj=0.0;
				for(int l=0;l<num_Entry;l++)
				{
					summ+=a[i][l]*a[j][l];
					sumi+=a[i][l];
					sumj+=a[j][l];
				}
				su[i][j]=summ/Math.max(Math.min(sumi,sumj), eps);
			}
		for(int i=0;i<num_User;i++)
			for(int j=0;j<num_Entry;j++)
			{
				double summ=0.0;
				double sumsu=0.0;
				for(int l=0;l<num_User;l++)
				{
					summ+=su[i][l]*a[l][j];
					sumsu+=su[i][l];
				}
				rcf[i][j]=summ/Math.max(sumsu, eps);
			}
		//UpdateDB(rcf,num_User,num_Entry);
		
	}
	public void updatePersonalRecommendations_BG() throws SQLException
	{//BIPARTITE GRAPH
		Statement stUserNumber=cnRecommender.createStatement();
		Statement stEntryNumber=cnRecommender.createStatement();
		ResultSet rsUserNumber=stUserNumber.executeQuery("select count(*) from movie_users");
		ResultSet rsEntryNumber=stEntryNumber.executeQuery("select max(id) from movies");
		rsUserNumber.next();
		//int num_User=rsUserNumber.getInt(1);
		int num_User=100;
		rsEntryNumber.next();
		int num_Entry=rsEntryNumber.getInt(1);
		//int num_Entry=500;
		stUserNumber.close();
		stEntryNumber.close();
		
	

		double [][] w=new double[num_Entry][num_Entry];
		int [] NI=new int[num_Entry];
		int [] NU=new int[num_User];
		double [][] BGs=new double[num_User][num_Entry];
		boolean [][] a=new boolean [num_User][num_Entry];
		Statement stAttention=cnRecommender.createStatement();
		ResultSet rsAttention=stAttention.executeQuery("SELECT user_id, movie_id, rate FROM movie_ratings");
		int user_id=0;
		while(rsAttention.next()&&user_id<num_User)
		{
			user_id=rsAttention.getInt(1);
			int movie_id=rsAttention.getInt(2);
			int rate=rsAttention.getInt(3);
			if(rate>=3)
			{
				a[user_id-1][movie_id-1]=true;
				NI[movie_id-1]++;
				NU[user_id-1]++;
			}
			
		}
		for(int i=0;i<num_Entry;i++)
			for(int j=0;j<num_Entry;j++)
			{
				double sum=0.0;
				for(int l=0;l<num_User;l++)
				{
					if(a[l][i]==true && a[l][j]==true)
					{
						sum+=1.0/(double)(NU[l]);
					}
				}
				w[i][j]=sum/NI[j];
			}
		double BGsThresh=0.0;
		for(int i=0;i<num_User;i++)
			for(int j=0;j<num_Entry;j++)
			{
				for(int l=0;l<num_Entry;l++)
				{
					if(a[i][l]==true)
					{
						BGs[i][j]+=w[j][l];
						BGsThresh+=w[j][l];
					}
					
				}
			}
		BGsThresh/=(double)(num_User*num_Entry);
		
		UpdateDB(BGs,num_User,num_Entry,BGsThresh);
		/*double [][] a=new double [num_User][num_Entry];//attention matrix
		double [][] rcf=new double[num_User][num_Entry];//Collaborate score
		double [][] su=new double[num_User][num_User];//user similarity matrix
		Statement stAttention=cnRecommender.createStatement();
		ResultSet rsAttention=stAttention.executeQuery("SELECT user_id, movie_id, rate FROM movie_ratings");
		while(rsAttention.next())
		{
			int user_id=rsAttention.getInt(1);
			int movie_id=rsAttention.getInt(2);
			a[user_id-1][movie_id-1]=Math.max((double)(rsAttention.getInt(3)/5.0), a[user_id-1][movie_id-1]);//maybe multiple attentions 
		}
		//calculate similarity scores between users
		for(int i=0;i<num_User;i++)
			for(int j=0;j<num_User;j++)
			{
				double summ=0.0;
				double sumi=0.0;
				double sumj=0.0;
				for(int l=0;l<num_Entry;l++)
				{
					summ+=a[i][l]*a[j][l];
					sumi+=a[i][l];
					sumj+=a[j][l];
				}
				su[i][j]=summ/Math.max(Math.min(sumi,sumj), eps);
			}
		for(int i=0;i<num_User;i++)
			for(int j=0;j<num_Entry;j++)
			{
				double summ=0.0;
				double sumsu=0.0;
				for(int l=0;l<num_User;l++)
				{
					summ+=su[i][l]*a[l][j];
					sumsu+=su[i][l];
				}
				rcf[i][j]=summ/Math.max(sumsu, eps);
			}
		*/
		
	}
	public void updatePersonalRecommendations_SCF() throws SQLException
	{
		Statement stUserNumber=cnRecommender.createStatement();
		Statement stEntryNumber=cnRecommender.createStatement();
		ResultSet rsUserNumber=stUserNumber.executeQuery("select count(*) from users");
		ResultSet rsEntryNumber=stEntryNumber.executeQuery("select count(*) from entries");
		rsUserNumber.next();
		int num_User=rsUserNumber.getInt(1);
		rsEntryNumber.next();
		int num_Entry=rsEntryNumber.getInt(1);
		stUserNumber.close();
		stEntryNumber.close();
		double [][] su=new double[num_User][num_User];//user similarity matrix
		double [][] a=new double[num_User][num_Entry];//attention matrix
		double [][] rcf=new double[num_User][num_Entry];//Collaborate Filtering score
		double [][] sd=new double[num_Entry][num_Entry];//doc similarity
		double [][] rscf=new double[num_User][num_Entry];//Semantic Collaborate Filtering score
		
		Statement stAttention=cnRecommender.createStatement();
		ResultSet rsAttention=stAttention.executeQuery("SELECT at.attentionable_id, at.attentionable_type,at.entry_id, (at.weight)*(ac.weight) as score FROM attentions at, action_types ac WHERE at.action_type=ac.action_type");
		while(rsAttention.next())
		{
			int attentionable_id=rsAttention.getInt(1);
			int entry_id=rsAttention.getInt(3);
			a[attentionable_id-1][entry_id-1]=Math.max(rsAttention.getDouble(4), a[attentionable_id-1][entry_id-1]);//maybe multiple attentions 
		}
		stAttention.close();
		Statement stDocSimilarity=cnRecommender.createStatement();
		ResultSet rsDocSimilarity=stDocSimilarity.executeQuery("select * from recommendations");
		while(rsDocSimilarity.next())
		{
			int entry_id=rsDocSimilarity.getInt(2);
			int dest_entry_id=rsDocSimilarity.getInt(3);
			double relevance=rsDocSimilarity.getDouble(5);
			sd[entry_id-1][dest_entry_id-1]=relevance;
		}
		stDocSimilarity.close();
		for(int i=0;i<num_Entry;i++)
			sd[i][i]=1.0;
		//calculate similarity scores between users
		for(int i=0;i<num_User;i++)
			for(int j=0;j<num_User;j++)
			{
				double summ=0.0;
				double sumi=0.0;
				double sumj=0.0;
				for(int l=0;l<num_Entry;l++)
				{
					summ+=a[i][l]*a[j][l];
					sumi+=a[i][l];
					sumj+=a[j][l];
				}
				su[i][j]=summ/Math.max(Math.min(sumi,sumj), eps);
			}
		for(int i=0;i<num_User;i++)
			for(int j=0;j<num_Entry;j++)
			{
				double summ=0.0;
				double sumsu=0.0;
				for(int l=0;l<num_User;l++)
				{
					summ+=su[i][l]*a[l][j];
					sumsu+=su[i][l];
				}
				rcf[i][j]=summ/Math.max(sumsu, eps);
			}
		for(int i=0;i<num_User;i++)
			for(int j=0;j<num_Entry;j++)
			{
				/*double summ=0.0;
				double sumrcf=0.0;
				for(int l=0;l<num_Entry;l++)
				{
					summ+=rcf[i][l]*sd[l][j];
					sumrcf+=rcf[i][l];
				}
				rscf[i][j]=summ/Math.max(sumrcf, eps);*/
				double max=0.0;
				for(int l=0;l<num_Entry;l++)
				{
					max=Math.max(max, rcf[i][l]*sd[l][j]);
				}
				rscf[i][j]=max;
			}
		//UpdateDB(rscf,num_User,num_Entry);
		
		
	}
	private void UpdateDB(double[][] score,int num_User, int num_Entry, double thresh) throws SQLException
	{
		PreparedStatement pstIsInTableRec = cnRecommender.prepareStatement(
				"SELECT count(*) FROM movie_pr where user_id=? and movie_id=?");
		PreparedStatement pstIsInTableAtt = cnRecommender.prepareStatement(
				"SELECT count(*) FROM movie_ratings where user_id=? and movie_id=?");

		PreparedStatement pstUpdatePersonalRec = cnRecommender.prepareStatement(
				"Update movie_pr set score=? where user_id=? and movie_id=?");
		PreparedStatement pstInsertPersonalRec = cnRecommender.prepareStatement(
				"Insert into movie_pr (user_id,movie_id,score) VALUES(?,?,?)");
		int num_update=0;
		int num_insert=0;
		
		for(int i=0;i<num_User;i++)
			for(int j=0;j<num_Entry;j++)
			{
				if(score[i][j]>2*thresh)
				{
					pstIsInTableRec.setInt(1, i+1);
					pstIsInTableRec.setInt(2, j+1);
					pstIsInTableAtt.setInt(1, i+1);
					pstIsInTableAtt.setInt(2, j+1);
					ResultSet isInTableRec=pstIsInTableRec.executeQuery();
					ResultSet isInTableAtt=pstIsInTableAtt.executeQuery();
					isInTableRec.next();
					isInTableAtt.next();
					if(isInTableAtt.getInt(1)==0)
					{
						if(isInTableRec.getInt(1)==0)
						{
							pstInsertPersonalRec.setInt(1, i+1);
							pstInsertPersonalRec.setInt(2, j+1);
							pstInsertPersonalRec.setDouble(3, score[i][j]);
							pstInsertPersonalRec.addBatch();
							num_insert++;
						}
						else
						{
							pstUpdatePersonalRec.setInt(2, i+1);
							pstUpdatePersonalRec.setInt(3, j+1);
							pstUpdatePersonalRec.setDouble(1, score[i][j]);
							pstUpdatePersonalRec.addBatch();
							num_update++;
						}
						if(num_insert%100==0)
						{
							pstInsertPersonalRec.executeBatch();
						}
						if(num_update%100==0)
						{
							pstUpdatePersonalRec.executeBatch();
						}
					}
					
				}
			}
		pstInsertPersonalRec.executeBatch();
		pstUpdatePersonalRec.executeBatch();
		pstIsInTableRec.close();
		pstUpdatePersonalRec.close();
		pstInsertPersonalRec.close();
	}
	private void WriteMovietoDB(String path)throws Exception
	{
		Statement stTruncMovie=cnRecommender.createStatement();
		stTruncMovie.executeQuery("TRUNCATE TABLE movies");
		PreparedStatement pstWritetoDB = cnRecommender.prepareStatement(
		"INSERT into movies (id,name,year,genre) VALUES(?,?,?,?)");
		FileReader fileread=new FileReader(path);
		BufferedReader bufread=new BufferedReader(fileread);
		String readline;
		int num=0;
		while((readline=bufread.readLine())!=null)
		{
			num++;
			String [] temp=readline.split("\\)::");
			pstWritetoDB.setString(4, temp[1]);
			pstWritetoDB.setInt(3, Integer.parseInt(temp[0].substring(temp[0].length()-4, temp[0].length())));
			String [] temp1=temp[0].substring(0, temp[0].length()-5).split("::");
			pstWritetoDB.setString(2, temp1[1]);
			pstWritetoDB.setInt(1, Integer.parseInt(temp1[0]));
		
			pstWritetoDB.addBatch();
			if(num%100==0)
				pstWritetoDB.executeBatch();
			
			
			
		}
		pstWritetoDB.executeBatch();
		stTruncMovie.close();
		pstWritetoDB.close();
		fileread.close();
		bufread.close();
		
		
		
	}
	private void WriteUsertoDB(String path)throws Exception
	{
		Statement stTruncUser=cnRecommender.createStatement();
		stTruncUser.executeQuery("TRUNCATE TABLE movie_users");
		PreparedStatement pstWritetoDB = cnRecommender.prepareStatement(
		"INSERT into movie_users (id,gender,age,occupation) VALUES(?,?,?,?)");
		FileReader fileread=new FileReader(path);
		BufferedReader bufread=new BufferedReader(fileread);
		String readline;
		int num=0;
		while((readline=bufread.readLine())!=null)
		{
			num++;
			String [] temp=readline.split("::");
			pstWritetoDB.setInt(1, Integer.parseInt(temp[0]));
			pstWritetoDB.setString(2, temp[1]);
			pstWritetoDB.setInt(3, Integer.parseInt(temp[2]));
			pstWritetoDB.setInt(4, Integer.parseInt(temp[3]));
			pstWritetoDB.addBatch();
			if(num%100==0)
				pstWritetoDB.executeBatch();
			
		}
		pstWritetoDB.executeBatch();
		stTruncUser.close();
		pstWritetoDB.close();
		fileread.close();
		bufread.close();
		
	}
	private void WriteRatetoDB(String path)throws Exception
	{
		Statement stTruncRate=cnRecommender.createStatement();
		stTruncRate.executeQuery("TRUNCATE TABLE movie_ratings");
		PreparedStatement pstWritetoDB = cnRecommender.prepareStatement(
		"INSERT into movie_ratings (user_id,movie_id,rate) VALUES(?,?,?)");
		FileReader fileread=new FileReader(path);
		BufferedReader bufread=new BufferedReader(fileread);
		String readline;
		int num=0;
		while((readline=bufread.readLine())!=null)
		{
			num++;
			String [] temp=readline.split("::");
			pstWritetoDB.setInt(1, Integer.parseInt(temp[0]));
			pstWritetoDB.setInt(2, Integer.parseInt(temp[1]));
			pstWritetoDB.setInt(3, Integer.parseInt(temp[2]));
			pstWritetoDB.addBatch();
			if(num%10000==0)
				pstWritetoDB.executeBatch();
			
		}
		pstWritetoDB.executeBatch();
		stTruncRate.close();
		pstWritetoDB.close();
		fileread.close();
		bufread.close();
		
	}
	public MovieDBPR(Connection cnRecommendered) throws Exception
	{
		cnRecommender=cnRecommendered;
		/*WriteMovietoDB("C:\\wang\\OERPROJECT\\movieDB\\movies.dat");
		WriteUsertoDB("C:\\wang\\OERPROJECT\\movieDB\\users.dat");
		WriteRatetoDB("C:\\wang\\OERPROJECT\\movieDB\\ratings.dat");*/
		
	}
}
