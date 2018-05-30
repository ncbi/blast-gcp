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

  // So we can efficiently compute topN scores by RID, use Java's Red Black
  // Tree to keep always sorted scores
  private HashMap<String, TreeMap<Double, ArrayList<BLAST_QUERY>>> score_map;

  public BLAST_TOPN() {
    score_map = new HashMap<>();
    logger = LogManager.getLogger(BLAST_DRIVER.class);

    logger.log(Level.INFO, String.format("TOPN class"));
  }

  public void add(String key, Double score, BLAST_QUERY line) {
    if (!score_map.containsKey(key)) {
      TreeMap<Double, ArrayList<BLAST_QUERY>> tm =
          new TreeMap<Double, ArrayList<BLAST_QUERY>>(Collections.reverseOrder());
      score_map.put(key, tm);
    }

    // FIX optimize: early cutoff if tm.size>topn
    TreeMap<Double, ArrayList<BLAST_QUERY>> tm = score_map.get(key);

    if (!tm.containsKey(score)) {
      ArrayList<BLAST_QUERY> al = new ArrayList<BLAST_QUERY>();
      tm.put(score, al);
    }
    ArrayList<BLAST_QUERY> al = tm.get(score);

    al.add(line);
    tm.put(score, al);
    score_map.put(key, tm);
  }

  public ArrayList<ArrayList<BLAST_QUERY>> results() {
    logger.log(Level.INFO, String.format("topn_ hashmap has %d", score_map.size()));
    if (score_map.size() > 1)
      logger.log(Level.WARN, String.format(" topn_ hashmap has > 1 RID:%d", score_map.size()));

    ArrayList<ArrayList<BLAST_QUERY>> results = new ArrayList<>();
    for (String key : score_map.keySet()) {
      TreeMap<Double, ArrayList<BLAST_QUERY>> tm = score_map.get(key);
      ArrayList<BLAST_QUERY> al = new ArrayList<BLAST_QUERY>();
      int i = 0;
      for (Double score : tm.keySet()) {
        if (i < 100) // FIX
        {
          for (BLAST_QUERY q : tm.get(score)) al.add(q);
        } else {
          logger.log(Level.DEBUG, " Skipping rest");
          break;
        }
        ++i;
      }
      results.add(al);
    }
    return results;
  }

  public static void main(String[] args) throws Exception {
    BLAST_TOPN topn = new BLAST_TOPN();

    for (double d = -5; d < 200; ++d) {
      Double dbl = d;
      BLAST_QUERY q = new BLAST_QUERY();
      q.setRid("test");
      topn.add("KEY1", d, q);
    }

    ArrayList<ArrayList<BLAST_QUERY>> results = topn.results();
    int i = 0;
    for (ArrayList<BLAST_QUERY> s : results) {
      int j = 0;
      for (BLAST_QUERY q : s) {
        String rid = q.getRid();
        System.out.println(
            String.format("rid %d (%s): result %d (%s): %s", i, rid, j, q.getRid(), q.toString()));
        ++j;
      }
      ++i;
    }
  }
}
