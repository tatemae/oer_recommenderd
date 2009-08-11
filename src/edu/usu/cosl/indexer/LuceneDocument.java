package edu.usu.cosl.indexer;

import java.util.Enumeration;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

import edu.usu.cosl.recommenderd.EntryInfo;

public class LuceneDocument {
  public static Document Document(EntryInfo entry)
       throws java.io.FileNotFoundException, Exception {
	 
	  try
	  {
	    Document doc = new Document();
	    
	    doc.add(new Field("id", "Entry:" + entry.nEntryID, Field.Store.YES, Field.Index.UN_TOKENIZED));
	    doc.add(new Field("type_s", "Entry", Field.Store.NO, Field.Index.UN_TOKENIZED));
	    doc.add(new Field("pk_i", "" + entry.nEntryID, Field.Store.YES, Field.Index.UN_TOKENIZED));
	    
//	    doc.add(new Field("feed_id_i", "" + entry.nFeedID, Field.Store.YES, Field.Index.UN_TOKENIZED));
//	    doc.add(new Field("permalink", entry.sURI, Field.Store.YES, Field.Index.UN_TOKENIZED));
//	    doc.add(new Field("direct_link", entry.sDirectLink == null ? "" : entry.sDirectLink, Field.Store.YES, Field.Index.UN_TOKENIZED));
//
//	    doc.add(new Field("title", entry.sTitle, Field.Store.YES, Field.Index.NO));
//	    doc.add(new Field("collection", entry.sFeedShortTitle, Field.Store.YES, Field.Index.NO));
	    
	    // give titles three times the weight as descriptions
	    doc.add(new Field("text", entry.sTitle, Field.Store.NO, Field.Index.TOKENIZED, Field.TermVector.YES));
	    doc.add(new Field("text", entry.sTitle, Field.Store.NO, Field.Index.TOKENIZED, Field.TermVector.YES));
	    doc.add(new Field("text", entry.sTitle, Field.Store.NO, Field.Index.TOKENIZED, Field.TermVector.YES));
	    doc.add(new Field("text", entry.sDescription, Field.Store.NO, Field.Index.TOKENIZED, Field.TermVector.YES));
	    
	    // give tags twice the weight as descriptions
	    for (Enumeration<String> eSubjects = entry.vSubjects.elements(); eSubjects.hasMoreElements();) {
	    	String sSubject = eSubjects.nextElement();
		    doc.add(new Field("text", sSubject, Field.Store.NO, Field.Index.TOKENIZED, Field.TermVector.YES));
		    doc.add(new Field("text", sSubject, Field.Store.NO, Field.Index.TOKENIZED, Field.TermVector.YES));
	    }

	    // tag is an unstemmed field used to autogenerate new tags when the metadata doesn't provide enough
	    doc.add(new Field("tag", entry.sTitle, Field.Store.NO, Field.Index.TOKENIZED, Field.TermVector.YES));
	    doc.add(new Field("tag", entry.sTitle, Field.Store.NO, Field.Index.TOKENIZED, Field.TermVector.YES));
	    doc.add(new Field("tag", entry.sTitle, Field.Store.NO, Field.Index.TOKENIZED, Field.TermVector.YES));
	    doc.add(new Field("tag", entry.sDescription, Field.Store.NO, Field.Index.TOKENIZED, Field.TermVector.YES));

	    // we need grain sizes so we can search just courses
	    doc.add(new Field("grain_size_s", entry.sGrainSize, Field.Store.NO, Field.Index.UN_TOKENIZED));
	    return doc;
	  }
	  catch (Exception e)
	  {
		  return null;
	  }
  }

  private LuceneDocument() {}
}
    
