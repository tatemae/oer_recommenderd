package edu.usu.cosl.recommenderd;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

public class LuceneDocument {
  public static Document Document(EntryInfo entry)
       throws java.io.FileNotFoundException, Exception {
	 
	  try
	  {
	    Document doc = new Document();
	    
	    doc.add(new Field("id", "Entry:" + entry.nEntryID, Field.Store.YES, Field.Index.UN_TOKENIZED));
	    doc.add(new Field("type_s", "Entry", Field.Store.NO, Field.Index.UN_TOKENIZED));
	    doc.add(new Field("pk_i", "" + entry.nEntryID, Field.Store.YES, Field.Index.UN_TOKENIZED));
	    
	    doc.add(new Field("feed_id_i", "" + entry.nFeedID, Field.Store.YES, Field.Index.UN_TOKENIZED));
	    doc.add(new Field("permalink", entry.sURI, Field.Store.YES, Field.Index.UN_TOKENIZED));
	    doc.add(new Field("direct_link", entry.sDirectLink == null ? "" : entry.sDirectLink, Field.Store.YES, Field.Index.UN_TOKENIZED));

	    doc.add(new Field("title", entry.sTitle, Field.Store.YES, Field.Index.NO));
	    doc.add(new Field("collection", entry.sFeedShortTitle, Field.Store.YES, Field.Index.NO));
	    
	    String sText = 
	    	entry.sTitle + " " + entry.sTitle + " " + entry.sTitle + " " +  
	    	entry.sDescription + " " + 
	    	entry.sTagList + " " + entry.sTagList;  
	    
	    doc.add(new Field("text", sText, Field.Store.NO, Field.Index.TOKENIZED, Field.TermVector.YES));
	    
	    doc.add(new Field("tag", sText, Field.Store.NO, Field.Index.TOKENIZED, Field.TermVector.YES));
	    return doc;
	  }
	  catch (Exception e)
	  {
		  return null;
	  }
  }

  private LuceneDocument() {}
}
    
