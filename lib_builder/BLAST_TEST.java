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

// import org.apache.logging.log4j.LogManager;
// import org.apache.logging.log4j.Logger;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

class BLAST_TEST {

  public static void main(String[] args) {
  Logger logger = LogManager.getLogger(BLAST_TEST.class);
    logger.info("Beginning");
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
    logger.trace(S);

    BLAST_REQUEST requestobj = new BLAST_REQUEST(rid + ":" + query, top_n);
    //        new BLAST_REQUEST(rid + ":" + query + ":nt:" + params + ":" + params, top_n);
    BLAST_PARTITION partitionobj = new BLAST_PARTITION(location, db_part, 14, true);

    BLAST_LIB blaster = new BLAST_LIB();

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
      // BLAST_TB_LIST.save(rid + ".seq-annot.asn1", oneasn);
      System.out.println("Dumped " + rid + " to file.");
    } else {
      System.out.println("NULL asn1");
    }
    logger.error("Finishing");
  }
}
