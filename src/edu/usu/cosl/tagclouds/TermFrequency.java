package edu.usu.cosl.tagclouds;

public class TermFrequency implements Comparable<TermFrequency> 
{
	public String sTerm;
	public int nFrequency;
	public boolean bSortOnName;
	
	public TermFrequency(String sTerm, int nFrequency) {
		this(sTerm,nFrequency,false);
	}
	public TermFrequency(String sTerm, int nFrequency, boolean bSortOnName) {
		this.sTerm = sTerm;
		this.nFrequency = nFrequency;
		this.bSortOnName = bSortOnName;
	}
	private int compareNames(TermFrequency tf) {
		return sTerm.compareTo(tf.sTerm);
	}
	public int compareFrequencies(TermFrequency tf) {
		int nOtherFreq = tf.nFrequency;
		if (nOtherFreq == nFrequency) return 0;
		else if (nOtherFreq < nFrequency) return -1;
		else return 1;
	}
	public int compareTo(TermFrequency tf) {
		if (bSortOnName) return compareNames(tf);
		else return compareFrequencies(tf);
	}
}

