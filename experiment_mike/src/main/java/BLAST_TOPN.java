/*===========================================================================
 *
 *                            PUBLIC DOMAIN NOTICE
 *               National Center for Biotechnology Information
 *
 *  This software/database is a "United States Government Work" under the
 *  terms of the United States Copyright Act.  It was written as part of
 *  the author's official duties as a United States Government employee and
 *  thus cannot be copyrighted.  This software/database is freely available
 *  to the public for use. The National Library of Medicine and the U.S.
 *  Government have not placed any restriction on its use or reproduction.
 *
 *  Although all reasonable efforts have been taken to ensure the accuracy
 *  and reliability of the software and data, the NLM and the U.S.
 *  Government do not and cannot warrant the performance or results that
 *  may be obtained by using this software or data. The NLM and the U.S.
 *  Government disclaim all warranties, express or implied, including
 *  warranties of performance, merchantability or fitness for any particular
 *  purpose.
 *
 *  Please cite the author in any work or product based on this material.
 *
 *===========================================================================
 *
 */

package gov.nih.nlm.ncbi.blastjni;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.TreeMap;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public final class BLAST_TOPN implements Serializable {
  private Logger logger;

  // So we can efficiently compute topN scores by RID
  private HashMap<String, TreeMap<Double, ArrayList<String>>> score_map;

  public BLAST_TOPN() {
    score_map = new HashMap<>();
    logger = LogManager.getLogger(BLAST_DRIVER.class);

    logger.log(Level.INFO, String.format("TOPN class"));
  }

  public void add(String key, Double score, String line) {
    if (!score_map.containsKey(key)) {
      TreeMap<Double, ArrayList<String>> tm =
          new TreeMap<Double, ArrayList<String>>(Collections.reverseOrder());
      score_map.put(key, tm);
    }

    // FIX optimize: early cutoff if tm.size>topn
    TreeMap<Double, ArrayList<String>> tm = score_map.get(key);

    if (!tm.containsKey(score)) {
      ArrayList<String> al = new ArrayList<String>();
      tm.put(score, al);
    }
    ArrayList<String> al = tm.get(score);

    al.add(line);
    tm.put(score, al);
    score_map.put(key, tm);
  }

  public ArrayList<String> results(int topn) {
    logger.log(Level.INFO, String.format("topn_ hashmap has %d", score_map.size()));
    if (score_map.size() > 1)
      logger.log(Level.WARN, String.format(" topn_ hashmap has > 1 RID:%d", score_map.size()));

    ArrayList<String> results = new ArrayList<String>(score_map.size());
    for (String key : score_map.keySet()) {
      TreeMap<Double, ArrayList<String>> tm = score_map.get(key);
      // JSON records tend to be either ~360 or ~1,200 bytes
      StringBuilder output = new StringBuilder(tm.size() * 400);
      int i = 0;
      for (Double score : tm.keySet()) {
        if (i < topn) {
          ArrayList<String> al = tm.get(score);
          for (String line : al) {
            output.append(line);
            output.append('\n');
          }
        } else {
          logger.log(Level.DEBUG, " Skipping rest");
          break;
        }
        ++i;
      }
      results.add(output.toString());
    }

    return results;
  }

  public static void main(String[] args) throws Exception {
    BLAST_TOPN topn = new BLAST_TOPN();

    for (double d = -5; d < 200; ++d) {
      Double dbl = d;
      topn.add("KEY1", d, dbl.toString());
    }

    ArrayList<String> results = topn.results(100);
    int i = 0;
    for (String s : results) {
      System.out.println(String.format("%d: %s", i, s));
      ++i;
    }
  }
}
