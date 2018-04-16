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
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkFiles;

/* FIX and GENERAL COMMENTS
 *
 *  First, things that should be fixed will be marked FIX, and other stuff
 *  may be marked CMT for observations.
 *
 *  Of course, all of the printing to System.err is only temporary and will
 *  have to go away, much like these comments. But there it is.
 *
 *  White space is good. It helps people to be able to distinguish things
 *  visually. Super-dense coding style is like using a dark blue foreground
 *  on a black background.
 */

public class BLAST_LIB {
  // FIX - if a BLAST_LIB object is instantiated as a static member of an outer class,
  // this is known to work (i.e. lock up the JVM to serialize)
  // So private constructor, and getInstance() Factory method returns Singleton

  // CMT -
  // See http://literatejava.com/jvm/fastest-threadsafe-singleton-jvm/
  // The inner class (aka Initialization-on-demand holder idiom) is
  // supposedly 25X faster than a synchronized method, but that is if JIT is
  // involved and 10,000+ calls to the singleton are requested.
  // In our case, this occurs twice per query, so only the JVM bytecode
  // interpreter is involved and performance (~400 ns) should be fine.
  //
  //
  // enum Singleton's seem to be the cleanest

  private synchronized void loadLibrary() {
    if (!initialized) {
      try {
        // Java will look for libblastjni.so
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
      initialized = true;
    }
  }

  public BLAST_LIB() {
    loadLibrary();
    processID = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
  }

  private boolean initialized = false;
  private String processID;
  private ExceptionInInitializerError invalid;

  void throwIfBad() {
    if (!initialized) throw invalid;
  }

  private synchronized void log_trace(String msg) {
    Logger logger = LogManager.getLogger(BLAST_LIB.class);
    if (logger.isTraceEnabled()) {
      msg = "BLAST (" + this.processID + ") " + msg;
      logger.trace(msg);
    }
  }

  private synchronized void log_debug(String msg) {
    Logger logger = LogManager.getLogger(BLAST_LIB.class);
    if (logger.isDebugEnabled()) {
      msg = "BLAST (" + this.processID + ") " + msg;
      logger.debug(msg);
    }
  }

  private synchronized void log_info(String msg) {
    Logger logger = LogManager.getLogger(BLAST_LIB.class);
    msg = "BLAST (" + this.processID + ") " + msg;
    logger.info(msg);
  }

  private synchronized void log_warn(String msg) {
    Logger logger = LogManager.getLogger(BLAST_LIB.class);
    msg = "BLAST (" + this.processID + ") " + msg;
    logger.warn(msg);
  }

  private synchronized void log_error(String msg) {
    Logger logger = LogManager.getLogger(BLAST_LIB.class);
    msg = "BLAST (" + this.processID + ") " + msg;
    logger.error(msg);
  }

  private synchronized void log_fatal(String msg) {
    Logger logger = LogManager.getLogger(BLAST_LIB.class);
    msg = "BLAST (" + this.processID + ") " + msg;
    logger.fatal(msg);
  }

  BLAST_HSP_LIST[] jni_prelim_search(BLAST_PARTITION part, BLAST_REQUEST req) {

    // CMT - I hadn't intended this to be used to guard every method, but it's safer to do so
    throwIfBad();

    // CMT - remember that white space is good. Imagine it like a sort of cryptocurrency mining tool
    log_info("\nJava jni_prelim_search called with");
    log_info("  query   : " + req.query);
    log_info("  db_spec : " + part.db_spec);
    log_info("  program : " + req.program);
    log_info("  params  : " + req.params);
    log_info("  topn    : " + req.top_n);

    long starttime = System.currentTimeMillis();
    BLAST_HSP_LIST[] ret =
        prelim_search(req.query, part.db_spec, req.program, req.params, req.top_n);

    long finishtime = System.currentTimeMillis();
    log_info("jni_prelim_search returned in " + (finishtime - starttime) + " ms.");
    log_info("jni_prelim_search returned " + ret.length + " HSP_LISTs:");
    int i = 0;
    for (BLAST_HSP_LIST h : ret) {
      h.part = part;
      h.req = req;
      log_debug("#" + i + ": " + h.toString());
      ++i;
    }

    // CMT - I think we will want to add some sort of a tracing facility gated upon verbosity
    // and perhaps some other switches so that we can look inside of a running cluster from
    // time to time. This is, of course, later.
    return ret;
  }

  BLAST_TB_LIST[] jni_traceback(BLAST_HSP_LIST[] hspl, BLAST_PARTITION part, BLAST_REQUEST req) {
    throwIfBad();

    log_info("\nJava jni_traceback called with");
    log_info("  query   : " + req.query);
    log_info("  db_spec : " + part.db_spec);
    log_info("  program : " + req.program);

    long starttime = System.currentTimeMillis();
    BLAST_TB_LIST[] ret = traceback(req.query, part.db_spec, req.program, hspl);
    long finishtime = System.currentTimeMillis();

    log_info("jni_traceback returned in " + (finishtime - starttime) + " ms.");
    log_info("jni_traceback returned " + ret.length + " TB_LISTs:");

    /*
        int i = 0;
        for (BLAST_TB_LIST t : ret) {
          t.part = part;
          t.req = req;
          ++i;
        }
    */
    return ret;
  }

  private native BLAST_HSP_LIST[] prelim_search(
      String query, String db_spec, String program, String params, int topn);

  private native BLAST_TB_LIST[] traceback(
      String query, String db_spec, String program, BLAST_HSP_LIST[] hspl);
}
