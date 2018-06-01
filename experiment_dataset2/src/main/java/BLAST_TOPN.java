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
import java.util.HashMap;
import java.util.TreeSet;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public final class BLAST_TOPN implements Serializable {
  private final Logger logger;

  // So we can efficiently compute topN scores by RID, use Java's Red Black
  // Tree to keep always sorted scores
  private final HashMap<String, TreeSet<Double>> score_map;
  private final HashMap<String, Integer> topns;

  public BLAST_TOPN() {
    score_map = new HashMap<>();
    topns = new HashMap<>();
    logger = LogManager.getLogger(BLAST_DRIVER.class);
  }

  public void add(String key, Double score, int topn) {
    if (topns.containsKey(key) && topns.get(key) != topn) {
      logger.log(Level.ERROR, "topn changed");
    } else topns.put(key, topn);

    if (!score_map.containsKey(key)) {
      TreeSet<Double> ts = new TreeSet<Double>();
      //                new TreeSet<Double>(Collections.reverseOrder());

      score_map.put(key, ts);
    }

    // FIX optimize: early cutoff if tm.size>topn
    TreeSet<Double> ts = score_map.get(key);

    ts.add(score);
    // score_map.put(key, ts);
  }

  public HashMap<String, Double> results() {
    logger.log(Level.DEBUG, String.format("topn_ hashmap has %d", score_map.size()));
    if (score_map.size() > 1)
      logger.log(Level.WARN, String.format(" topn_ hashmap has > 1 RID:%d", score_map.size()));

    HashMap<String, Double> r = new HashMap<>();
    for (String key : score_map.keySet()) {
      int topn = topns.get(key);
      TreeSet<Double> ts = score_map.get(key);
      int i = 0;
      Double cutoff = 0.0;
      for (Double score : ts.descendingSet()) {
        if (i < topn) {
          cutoff = score;
        } else {
          break;
        }
        ++i;
      }
      r.put(key, cutoff);
    }
    return r;
  }

  public static void main(String[] args) throws Exception {
    BLAST_TOPN topn = new BLAST_TOPN();

    for (double d = -5; d < 200; ++d) {
      Double dbl = d;
      topn.add("test1", dbl, 100);
      topn.add("test2", dbl, 10);
    }

    HashMap<String, Double> r = topn.results();

    r.toString();
    for (String k : r.keySet()) {
      System.out.println(k);
      System.out.println(r.get(k));
    }
  }
}
