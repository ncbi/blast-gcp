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

class GCP_BLAST_LIB {

  // FIX - we need to ensure that this is locked down with regard to multi-threading
  // if a GCP_BLAST_LIB object is instantiated as a static member of an outer class,
  // this is known to work (i.e. lock up the JVM to serialize)

  GCP_BLAST_LIB() {
    System.err.println("In Java GCP_BLAST_LIB ctor");
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
  }

  void throwIfBad() {
    if (invalid != null) {
      System.err.println("invalid");
      throw invalid;
    }
  }

  public synchronized void setLogWriter(PrintWriter writer) {
    log_writer = writer;
  }

  // Warning: If below signature changes, update blastjni.cpp
  // CMT - not really a fix, but an observation that now that you're
  // generating signatures, you might booby-trap the build script to
  // catch any deviation in the signature of this method. And I'd
  // make the warning MUCH harder to avoid seeing/reading.
  private synchronized void log(String msg) {
    if (log_writer != null) {
      try {
        log_writer.println(msg);
        log_writer.flush();
      } catch (Throwable t) {
      }
    } else {
      System.err.println(msg);
    }
  }

  GCP_BLAST_HSP_LIST[] jni_prelim_search( GCP_BLAST_PARTITION part, GCP_BLAST_REQUEST req ) 
  {

    // CMT - I hadn't intended this to be used to guard every method, but it's safer to do so
    throwIfBad();

    // CMT - remember that white space is good. Imagine it like a sort of cryptocurrency mining tool
    /*
    log("\nJava jni_prelim_search called with");
    log("  query   : " + query);
    log("  db_spec : " + db_spec);
    log("  program : " + program);
    log("  params  : " + params);
    log("  topn    : " + topn);
    */

    long starttime = System.currentTimeMillis();
    GCP_BLAST_HSP_LIST[] ret = prelim_search( req.query, part.db_spec, req.program, req.params, req.top_n );

    long finishtime = System.currentTimeMillis();
    log("jni_prelim_search returned in " + (finishtime - starttime) + " ms.");
    log("jni_prelim_search returned " + ret.length + " HSP_LISTs:");
    int i = 0;
    for (GCP_BLAST_HSP_LIST h : ret) {
      h.partitionobj = part;
      h.requestobj = req;
      //log("#" + i + ": " + h.toString());
      ++i;
    }

    // CMT - I think we will want to add some sort of a tracing facility gated upon verbosity
    // and perhaps some other switches so that we can look inside of a running cluster from
    // time to time. This is, of course, later.

    return ret;
  }

  GCP_BLAST_TB_LIST[] jni_traceback(
      GCP_BLAST_HSP_LIST[] hspl,
      GCP_BLAST_PARTITION partitionobj,
      GCP_BLAST_REQUEST requestobj) {
    throwIfBad();

    /*
    log("\nJava jni_traceback called with");
    log("  query   : " + requestobj.query );
    log("  db_spec : " + partitionobj.db_spec);
    log("  program : " + requestobj.program );
    */
    long starttime = System.currentTimeMillis();
    GCP_BLAST_TB_LIST[] ret = traceback( requestobj.query, partitionobj.db_spec, requestobj.program, hspl );
    long finishtime = System.currentTimeMillis();
    /*
    log("jni_traceback returned in " + (finishtime - starttime) + " ms.");
    log("jni_traceback returned " + ret.length + " TB_LISTs:");
    */
    int i = 0;
    for (GCP_BLAST_TB_LIST t : ret) {
      t.partitionobj = partitionobj;
      t.requestobj = requestobj;
      //log("#" + i + ": " + t.toString());
      ++i;
    }

    return ret;
  }

  private ExceptionInInitializerError invalid;
  private PrintWriter log_writer;

  private native GCP_BLAST_HSP_LIST[] prelim_search(
      String query, String db_spec, String program, String params, int topn);

  private native GCP_BLAST_TB_LIST[] traceback(
      String query, String db_spec, String program, GCP_BLAST_HSP_LIST[] hspl);

  public static void main(String[] args) {
    /*
    final String username = System.getProperty("user.name");

    String rid = "ReqID123";
    String query = "CCGCAAGCCAGAGCAACAGCTCTAACAAGCAGAAATTCTGACCAAACTGATCCGGTAAAACCGATCAACG";
    String part =
        "/panfs/pan1.be-md.ncbi.nlm.nih.gov/blastprojects/GCP_blastdb/50M/nt_50M.14"; // 14 & 18
    // have hits
    String params = "nt";
    String program = "blastn";

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

    GCP_BLAST_REQUEST requestobj =
        new GCP_BLAST_REQUEST(rid + ":" + query + ":nt:" + params + ":" + params);
    GCP_BLAST_PARTITION partitionobj = new GCP_BLAST_PARTITION("", "", 0, true);

    GCP_BLAST_LIB blaster = new GCP_BLAST_LIB();

    try {
      PrintWriter pw = new PrintWriter(new FileOutputStream(new File("/tmp/blastjni.log"), true));
      blaster.setLogWriter(pw);
    } catch (FileNotFoundException ex) {
      System.err.println("Couldn't create log");
    }

    GCP_BLAST_HSP_LIST hspl[] =
        blaster.jni_prelim_search(query, part, program, params, 100, partitionobj, requestobj);
    System.out.println("--- PRELIM_SEARCH RESULTS ---");
    if (hspl != null) {
      System.out.println(" prelim_search returned " + hspl.length + " HSP lists:");
      for (GCP_BLAST_HSP_LIST hsp : hspl) {
        System.out.println(hsp.toString());
      }
    } else {
      System.out.println("NULL hspl");
    }

    GCP_BLAST_TB_LIST[] tbs =
        blaster.jni_traceback(query, part, program, hspl, partitionobj, requestobj);

    System.out.println("traceback done");
    System.out.println("--- TRACEBACK RESULTS ---");

    if (tbs != null) {
      for (GCP_BLAST_TB_LIST tb : tbs) {
        System.out.println(tb.toString());
      }

      byte[][] oneasn = new byte[1][0];
      oneasn[0] = tbs[0].asn1_blob;
      GCP_BLAST_TB_LIST.save(rid + ".seq-annot.asn1", oneasn);
      System.out.println("Dumped " + rid + " to file.");
    } else {
      System.out.println("NULL asn1");
    }
    */
  }
}
