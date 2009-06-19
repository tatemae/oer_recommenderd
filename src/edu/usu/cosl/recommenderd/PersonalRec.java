package edu.usu.cosl.recommenderd;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;

import java.lang.Math;

import edu.usu.cosl.util.DBThread;
import edu.usu.cosl.util.Logger;

public class PersonalRec extends DBThread
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
		double [][] rcf=new double[num_User][num_Entry];//Collaborate score
		Statement stAttention=cnRecommender.createStatement();
		ResultSet rsAttention=stAttention.executeQuery("SELECT at.attentionable_id, at.attentionable_type,at.entry_id, (at.weight)*(ac.weight) as score FROM attentions at, action_types ac WHERE at.action_type=ac.action_type");
		while(rsAttention.next())
		{
			int attentionable_id=rsAttention.getInt(1);
			int entry_id=rsAttention.getInt(3);
			a[attentionable_id-1][entry_id-1]=Math.max(rsAttention.getDouble(4), a[attentionable_id-1][entry_id-1]);//maybe multiple attentions 
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
		UpdateDB(rcf,num_User,num_Entry);
		
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
			sd[rsDocSimilarity.getInt(2)-1][rsDocSimilarity.getInt(3)-1]=rsDocSimilarity.getDouble(5);
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
				double summ=0.0;
				double sumrcf=0.0;
				for(int l=0;l<num_Entry;l++)
				{
					summ+=rcf[i][l]*sd[l][j];
					sumrcf+=rcf[i][l];
				}
				rscf[i][j]=summ/Math.max(sumrcf, eps);
			}
		UpdateDB(rscf,num_User,num_Entry);
		
		
	}
	private void UpdateDB(double[][] score,int num_User, int num_Entry) throws SQLException
	{
		PreparedStatement pstIsInTableRec = cnRecommender.prepareStatement(
				"SELECT count(*) FROM personal_recommendations p where p.personal_recommendable_id=? and p.destination_id=?");
		PreparedStatement pstIsInTableAtt = cnRecommender.prepareStatement(
				"SELECT count(*) FROM attentions where attentionable_id=? and entry_id=?");

		PreparedStatement pstUpdatePersonalRec = cnRecommender.prepareStatement(
				"Update personal_recommendations set relevance=? where personal_recommendable_id=? and destination_id=?");
		PreparedStatement pstInsertPersonalRec = cnRecommender.prepareStatement(
				"Insert into personal_recommendations (personal_recommendable_id,destination_id,relevance) VALUES(?,?,?)");
		int num_update=0;
		int num_insert=0;
		
		for(int i=0;i<num_User;i++)
			for(int j=0;j<num_Entry;j++)
			{
				if(score[i][j]>0)
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
	public PersonalRec(Connection cnRecommendered) throws Exception
	{
		cnRecommender=cnRecommendered;
	}
}
