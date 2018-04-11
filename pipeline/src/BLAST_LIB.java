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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import org.apache.spark.SparkFiles;

// import org.apache.log4j.Level;

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

class BLAST_LIB {
  // TODO: Replace with enum or Log4j's Level
  public static final int LOG_TRACE = 0;
  public static final int LOG_DEBUG = 1;
  public static final int LOG_INFO = 2;
  public static final int LOG_WARN = 3;
  public static final int LOG_ERROR = 4;
  public static final int LOG_FATAL = 5;

  // FIX - we need to ensure that this is locked down with regard to multi-threading
  // if a BLAST_LIB object is instantiated as a static member of an outer class,
  // this is known to work (i.e. lock up the JVM to serialize)

  BLAST_LIB() {
    this.loglevel = BLAST_LIB.LOG_INFO;
    System.err.println("In Java BLAST_LIB ctor");
    try {
      // Java will look for libblastjni.so
      System.loadLibrary("blastjni");
    } catch (Throwable e) {
      try {
        System.err.println("Nope #1");
        System.load(SparkFiles.get("libblastjni.so"));
      } catch (ExceptionInInitializerError x) {
        System.err.println("Nope #2");
        invalid = x;
      } catch (Throwable e2) {
        System.err.println("Nope #3");
        invalid = new ExceptionInInitializerError(e2);
      }
    }

    String username = System.getProperty("user.name", "unk");
    String log_filename = "/tmp/blastjni." + username + ".log";

    try {
      // FIX - When Dataproc/Spark goes to Java 9+, replace with
      // Process.getPid()
      this.processID = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
      PrintWriter pw = new PrintWriter(new FileOutputStream(new File(log_filename), true));
      this.setLogWriter(pw);
    } catch (FileNotFoundException ex) {
      System.err.println("Couldn't create log");
    }
  }

  void throwIfBad() {
    if (invalid != null) {
      System.err.println("invalid");
      throw invalid;
    }
  }

  public void setLogLevel(int level) {
    if (level >= BLAST_LIB.LOG_TRACE && level <= BLAST_LIB.LOG_FATAL) this.loglevel = level;
  }

  public synchronized void setLogWriter(PrintWriter writer) {
    log_writer = writer;
  }

  // Warning: If below signature changes, update blastjni.cpp
  // CMT - not really a fix, but an observation that now that you're
  // generating signatures, you might booby-trap the build script to
  // catch any deviation in the signature of this method. And I'd
  // make the warning MUCH harder to avoid seeing/reading.
  private synchronized void log(int level, String msg) {
    if (log_writer != null && level >= this.loglevel) {
      try {
        String levelstr;
        switch (level) {
          case LOG_TRACE:
            levelstr = "TRACE";
            break;
          case LOG_DEBUG:
            levelstr = "DEBUG";
            break;
          case LOG_INFO:
            levelstr = "INFO";
            break;
          case LOG_WARN:
            levelstr = "WARN";
            break;
          case LOG_ERROR:
            levelstr = "ERROR";
            break;
          case LOG_FATAL:
            levelstr = "FATAL";
            break;
          default:
            levelstr = "UNKOWNLEVEL";
            break;
        }
        msg = "(" + this.processID + ") [" + levelstr + "] " + msg;
        log_writer.println(msg);
        log_writer.flush();
      } catch (Throwable t) {
      }
    } else {
      System.err.println(msg);
    }
  }

  BLAST_HSP_LIST[] jni_prelim_search(BLAST_PARTITION part, BLAST_REQUEST req) {

    // CMT - I hadn't intended this to be used to guard every method, but it's safer to do so
    throwIfBad();

    // CMT - remember that white space is good. Imagine it like a sort of cryptocurrency mining tool
    /*
    log( "\nJava jni_prelim_search called with" );
    log( "  query   : " + query );
    log( "  db_spec : " + db_spec );
    log( "  program : " + program );
    log( "  params  : " + params );
    log( "  topn    : " + topn );
    */

    long starttime = System.currentTimeMillis();
    BLAST_HSP_LIST[] ret =
        prelim_search(req.query, part.db_spec, req.program, req.params, req.top_n);

    long finishtime = System.currentTimeMillis();
    // log( "jni_prelim_search returned in " + ( finishtime - starttime ) + " ms." );
    // log( "jni_prelim_search returned " + ret.length + " HSP_LISTs:" );
    int i = 0;
    for (BLAST_HSP_LIST h : ret) {
      h.part = part;
      h.req = req;
      // log( "#" + i + ": " + h.toString() );
      ++i;
    }

    // CMT - I think we will want to add some sort of a tracing facility gated upon verbosity
    // and perhaps some other switches so that we can look inside of a running cluster from
    // time to time. This is, of course, later.
    return ret;
  }

  BLAST_TB_LIST[] jni_traceback(BLAST_HSP_LIST[] hspl, BLAST_PARTITION part, BLAST_REQUEST req) {
    throwIfBad();

    /*
    log("\nJava jni_traceback called with");
    log("  query   : " + requestobj.query );
    log("  db_spec : " + partitionobj.db_spec);
    log("  program : " + requestobj.program );
    */
    long starttime = System.currentTimeMillis();
    BLAST_TB_LIST[] ret = traceback(req.query, part.db_spec, req.program, hspl);
    long finishtime = System.currentTimeMillis();
    /*
    log("jni_traceback returned in " + (finishtime - starttime) + " ms.");
    log("jni_traceback returned " + ret.length + " TB_LISTs:");
    */
    int i = 0;
    for (BLAST_TB_LIST t : ret) {
      t.part = part;
      t.req = req;
      // log( "#" + i + ": " + t.toString() );
      ++i;
    }

    return ret;
  }

  private ExceptionInInitializerError invalid;
  private PrintWriter log_writer;
  private String processID;
  private int loglevel;

  private native BLAST_HSP_LIST[] prelim_search(
      String query, String db_spec, String program, String params, int topn);

  private native BLAST_TB_LIST[] traceback(
      String query, String db_spec, String program, BLAST_HSP_LIST[] hspl);

  public static void main(String[] args) {
    String rid = "ReqID123";
    String query = "CCGCAAGCCAGAGCAACAGCTCTAACAAGCAGAAATTCTGACCAAACTGATCCGGTAAAACCGATCAACG";
    String part =
        "/panfs/pan1.be-md.ncbi.nlm.nih.gov/blastprojects/GCP_blastdb/50M/nt_50M.14"; // 14 & 18
    final String location = "/panfs/pan1.be-md.ncbi.nlm.nih.gov/blastprojects/GCP_blastdb/50M/";
    final String db_part = "nt_50M";
    // have hits
    String params = "nt";
    final String program = "blastn";
    final Integer top_n = 100;

    if (args.length > 0) {
      String[] req = args[0].split("\\:");
      if (req.length > 0) part = req[0];
      if (req.length > 1) rid = req[1];
      if (req.length > 2) query = req[2];
      if (req.length > 3) params = req[3];
    }

    String S = String.format("partition ... %s\n", part);
    S = S + String.format("req-id ...... %s\n", rid);
    S = S + String.format("query ....... %s\n", query);
    S = S + String.format("params ...... %s\n", params);
    System.err.println(S);

    BLAST_REQUEST requestobj = new BLAST_REQUEST(rid + ":" + query, top_n);
    //        new BLAST_REQUEST(rid + ":" + query + ":nt:" + params + ":" + params, top_n);
    BLAST_PARTITION partitionobj = new BLAST_PARTITION(location, db_part, 14, true);

    BLAST_LIB blaster = new BLAST_LIB();
    blaster.setLogLevel(LOG_INFO);

    BLAST_HSP_LIST hspl[] = blaster.jni_prelim_search(partitionobj, requestobj);
    System.out.println("--- PRELIM_SEARCH RESULTS ---");
    if (hspl != null) {
      System.out.println(" prelim_search returned " + hspl.length + " HSP lists:");
      for (BLAST_HSP_LIST hsp : hspl) {
        System.out.println(hsp.toString());
      }
    } else {
      System.out.println("NULL hspl");
    }

    BLAST_TB_LIST[] tbs = blaster.jni_traceback(hspl, partitionobj, requestobj);

    System.out.println("traceback done");
    System.out.println("--- TRACEBACK RESULTS ---");

    if (tbs != null) {
      for (BLAST_TB_LIST tb : tbs) {
        System.out.println(tb.toString());
      }

      byte[][] oneasn = new byte[1][0];
      oneasn[0] = tbs[0].asn1_blob;
      BLAST_TB_LIST.save(rid + ".seq-annot.asn1", oneasn);
      System.out.println("Dumped " + rid + " to file.");
    } else {
      System.out.println("NULL asn1");
    }
  }
}
