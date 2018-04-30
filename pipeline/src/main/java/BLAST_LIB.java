/*
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
 */

package gov.nih.nlm.ncbi.blastjni;

import java.lang.management.ManagementFactory;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkFiles;

public class BLAST_LIB {
  // Guaranteed to be a singleton courtesy of BLAST_LIB_SINGLETON
  public BLAST_LIB() {
    try {
      // Java will look for libblastjni.so, thread safe if successful?
      System.loadLibrary("blastjni");
    } catch (Throwable e) {
      try {
        System.load(SparkFiles.get("libblastjni.so"));
      } catch (ExceptionInInitializerError x) {
        invalid = x;
      } catch (Throwable e2) {
        invalid = new ExceptionInInitializerError(e2);
      }
    }
    processID = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
    logLevel = Level.INFO;
  }

  private static String processID;
  private static ExceptionInInitializerError invalid;
  private static Level logLevel;

  final void throwIfBad() {
    if (invalid != null)
    {throw invalid;
    }
  }

  // We can't rely on log4j.properties to filter, instead we'll look at
  // logLevel
  private void log(final String level, final String msg) {
    Logger logger = LogManager.getLogger(BLAST_LIB.class);
    Level l = Level.toLevel(level);

    if (l.isGreaterOrEqual(logLevel)) {
      final String newmsg = "BLASTJNI (" + BLAST_LIB.processID + ") " + msg;
      logger.log(l, newmsg);
    }
  }

  final BLAST_HSP_LIST[] jni_prelim_search( final BLAST_PARTITION part, final BLAST_REQUEST req, final String logLevel )
  {

    // CMT - I hadn't intended this to be used to guard every method, but it's safer to do so
    throwIfBad();

    BLAST_LIB.logLevel = Level.toLevel(logLevel);

    // CMT - remember that white space is good. Imagine it like a sort of cryptocurrency mining tool
    log("INFO", "Java jni_prelim_search called with");
    log("INFO", "  query   : " + req.query);
    log("INFO", "  db_spec : " + part.db_spec);
    log("INFO", "  program : " + req.program);
    // FIX - top_n_prelim
    log("INFO", "  topn    : " + req.top_n);

    if (req.query.contains("\n"))
    {
        log("WARN", "Query contains newline, which may crash Blast library");
    }

    long starttime = System.currentTimeMillis();

    BLAST_HSP_LIST[] ret =
        prelim_search(req.query, part.db_spec, req.program, req.params, req.top_n);

    long finishtime = System.currentTimeMillis();
    log("INFO", "jni_prelim_search returned in " + (finishtime - starttime) + " ms.");
    log("INFO", "jni_prelim_search returned " + ret.length + " HSP_LISTs:");
    int i = 0;
    for (BLAST_HSP_LIST h : ret) {
      h.part = part;
      h.req = req;
      log("DEBUG", "#" + i + ": " + h.toString());
      ++i;
    }

    return ret;
  }

  final BLAST_TB_LIST[] jni_traceback(
      BLAST_HSP_LIST[] hspl, final BLAST_PARTITION part, final BLAST_REQUEST req, final String logLevel) {
    throwIfBad();

    BLAST_LIB.logLevel = Level.toLevel(logLevel);
    log("INFO", "Java jni_traceback called with");
    log("INFO", "  query   : " + req.query);
    log("INFO", "  db_spec : " + part.db_spec);

    long starttime = System.currentTimeMillis();
    BLAST_TB_LIST[] ret = traceback(hspl, req.query, part.db_spec, req.program, req.params);
    long finishtime = System.currentTimeMillis();

    log("INFO", "jni_traceback returned in " + (finishtime - starttime) + " ms.");
    log("INFO", "jni_traceback returned " + ret.length + " TB_LISTs:");

    int i = 0;
    for (BLAST_TB_LIST t : ret) {
      t.part = part;
      t.req = req;
      ++i;
    }

    return ret;
  }

  private native BLAST_HSP_LIST[] prelim_search(
      String query, String db_spec, String program, String params, int top_n);

  private native BLAST_TB_LIST[] traceback(
      BLAST_HSP_LIST[] hspl, String query, String db_spec, String program, String params);
}
