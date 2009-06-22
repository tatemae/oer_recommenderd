package edu.usu.cosl.recommenderd;

public class TermFrequency implements Comparable 
{
	String sTerm;
	int nFrequency;
	boolean bSortOnName;
	
	public TermFrequency(String sTerm, int nFrequency) {
		this(sTerm,nFrequency,false);
	}
	public TermFrequency(String sTerm, int nFrequency, boolean bSortOnName) {
		this.sTerm = sTerm;
		this.nFrequency = nFrequency;
		this.bSortOnName = bSortOnName;
	}
	private int compareNames(Object o) {
		return sTerm.compareTo(((TermFrequency)o).sTerm);
	}
	public int compareFrequencies(Object o) {
		int nOtherFreq = ((TermFrequency)o).nFrequency;
		if (nOtherFreq == nFrequency) return 0;
		else if (nOtherFreq < nFrequency) return -1;
		else return 1;
	}
	public int compareTo(Object o) {
		if (bSortOnName) return compareNames(o);
		else return compareFrequencies(o);
	}
}

