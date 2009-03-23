/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/* copied and adapted from org.apache.solr.analysis.EnglishPorterFilterFactory */

package edu.usu.cosl.analysis.es;

import org.apache.solr.common.ResourceLoader;
import org.apache.solr.util.plugin.ResourceLoaderAware;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.Token;
import org.apache.solr.analysis.BaseTokenFilterFactory;

import java.util.List;
import java.util.Set;
import java.io.IOException;

/**
 * @version $Id: SpanishPorterFilterFactory.java 
 */
public class SpanishPorterFilterFactory extends BaseTokenFilterFactory implements ResourceLoaderAware {
  
  public void inform(ResourceLoader loader) {
    String wordFile = args.get("protected");
    if (wordFile != null) {
      try {
        List<String> wlist = loader.getLines(wordFile);
         protectedWords = StopFilter.makeStopSet((String[])wlist.toArray(new String[0]));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private Set protectedWords = null;

  public SpanishPorterFilter create(TokenStream input) {
    return new SpanishPorterFilter(input,protectedWords);
  }

}


/** English Porter2 filter that doesn't use reflection to
/*  adapt lucene to the snowball stemmer code.
 */
class SpanishPorterFilter extends TokenFilter {
  private final Set protWords;
  private net.sf.snowball.ext.SpanishStemmer stemmer;

  public SpanishPorterFilter(TokenStream source, Set protWords) {
    super(source);
    this.protWords=protWords;
    stemmer = new net.sf.snowball.ext.SpanishStemmer();
  }


  @Override
  public Token next() throws IOException {
    Token tok = input.next();
    if (tok==null) return null;
    String tokstr = tok.termText();

    // if protected, don't stem.  use this to avoid stemming collisions.
    if (protWords != null && protWords.contains(tokstr)) {
      return tok;
    }

    stemmer.setCurrent(tokstr);
    stemmer.stem();
    String newstr = stemmer.getCurrent();
    if (tokstr.equals(newstr)) {
      return tok;
    } else {
      // TODO: it would be nice if I could just set termText directly like
      // lucene packages can.
      Token newtok = new Token(newstr, tok.startOffset(), tok.endOffset(), tok.type());
      newtok.setPositionIncrement(tok.getPositionIncrement());
      return newtok;
    }

  }
}

