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

class ConcLoad extends Thread {
    private BLAST_PARTITION part;

    public ConcLoad(BLAST_PARTITION part) {
        this.part = part;
    }

    public void run() {
        System.out.println("thread started");
        //    BLAST_SETTINGS bls = SETTINGS.getValue();
        //   BLAST_LIB blaster = BLAST_LIB.get_lib(part, bls);
    }
}

class BLAST_TEST {

    public static void main(String[] args) 
        throws Exception
    {
        Logger logger = LogManager.getLogger(BLAST_TEST.class);
        logger.info("Beginning");
        String rid = "ReqID123";
        String query_seq = "CCGCAAGCCAGAGCAACAGCTCTAACAAGCAGAAATTCTGACCAAACTGATCCGGTAAAACCGATCAACG";
        //String query_url = "gs://blast-largequeries/query-022346762.txt";
        String query_url = "";

        String part =
            "/panfs/pan1.be-md.ncbi.nlm.nih.gov/blastprojects/GCP_blastdb/50M/nt_50M.14"; // 14 & 18
        final String location = "/panfs/pan1.be-md.ncbi.nlm.nih.gov/blastprojects/GCP_blastdb/50M/";
        final String db_part = "nt_50M";
        final String program = "blastn";

        String params = "{";
        params += "\n\"version\": 1,";
        params += "\n \"RID\": \"" + rid + "\" ,";
        params += "\n \"blast_params\": { \"todo\": \"todo\" } }";
        params += "\n";

        final Integer top_n = 100;

        if (args.length > 0) {
            String[] req = args[0].split("\\:");
            if (req.length > 0) part = req[0];
            if (req.length > 1) rid = req[1];
            if (req.length > 2) query_seq = req[2];
            if (req.length > 3) params = req[3];
        }

        String S = String.format("partition ... %s\n", part);
        S = S + String.format("req-id ...... %s\n", rid);
        S = S + String.format("query ....... %s\n", query_seq);
        S = S + String.format("params ...... %s\n", params);
        logger.info(S);

        BLAST_REQUEST requestobj = new BLAST_REQUEST();
        requestobj.id = rid;
        requestobj.query_seq = query_seq;
        requestobj.query_url = query_url;
        requestobj.params = params;
        requestobj.db = params;
        requestobj.program = program;
        requestobj.top_n = top_n;
        BLAST_PARTITION partitionobj = new BLAST_PARTITION(location, db_part, 14, true);

        //    for (int i = 0; i != 10; ++i) {
        //      ConcLoad p = new ConcLoad( partitionobj );
        //      p.start();
        //    }

        // BLAST_SETTINGS bls = BLAST_SETTINGS.getValue();
        // BLAST_LIB blaster = BLAST_LIB_SINGLETON.get_lib(partitionobj, bls);
        System.out.println("Creating blaster");
        BLAST_LIB blaster = new BLAST_LIB();
        System.out.println("Created  blaster");

        final String logLevel = "DEBUG";
        params = "nt"; // FIX - When Blast team ready for JSON params

        BLAST_HSP_LIST hspl[] = blaster.jni_prelim_search(partitionobj, requestobj, logLevel);
        System.out.println("--- PRELIM_SEARCH RESULTS ---");
        if (hspl != null) {
            System.out.println(" prelim_search returned " + hspl.length + " HSP lists:");
            for (BLAST_HSP_LIST hsp : hspl) {
                System.out.println("HSP: " + hsp.toString().replace("\n"," "));
            }
        } else {
            System.out.println("NULL hspl");
        }

        BLAST_TB_LIST[] tbs = blaster.jni_traceback(hspl, partitionobj, requestobj, logLevel);

        System.out.println("traceback done");
        System.out.println("--- TRACEBACK RESULTS ---");

        if (tbs != null) {
            for (BLAST_TB_LIST tb : tbs) {
                System.out.println("TB: " + tb.toString().replace("\n"," "));
            }

            byte[][] oneasn = new byte[1][0];
            oneasn[0] = tbs[0].asn1_blob;
            // BLAST_TB_LIST.save(rid + ".seq-annot.asn1", oneasn);
            System.out.println("Dumped " + rid + " to file.");
        } else {
            System.out.println("NULL asn1");
        }
        logger.info("Finishing");
    }
}
