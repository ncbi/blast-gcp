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

class ConcDoit extends Thread {
    int part_num;
    BLAST_REQUEST requestobj;
    BLAST_PARTITION partitionobj;

    public ConcDoit(int part_num, BLAST_REQUEST requestobj, BLAST_PARTITION partitionobj) {
        this.part_num = part_num;
        this.requestobj = requestobj;
        this.partitionobj = partitionobj;
    }

    public void run() {
        System.out.println("thread " + part_num + " started.");

        final String logLevel = "INFO";

        long starttime = System.currentTimeMillis();
        BLAST_LIB blaster = new BLAST_LIB();

        try {
            BLAST_HSP_LIST hspl[] = blaster.jni_prelim_search(partitionobj, requestobj, logLevel);
            if (hspl != null) {
                System.out.println(" prelim_search returned " + hspl.length + " HSP lists:");
                for (BLAST_HSP_LIST hsp : hspl) {
                    // System.out.println(hsp.toString());
                }
            } else {
                System.out.println("NULL hspl");
            }
            long middletime = System.currentTimeMillis();
            System.out.println(
                    "Partition "
                    + part_num
                    + " finished prelim_search in "
                    + (middletime - starttime)
                    + " ms");

            BLAST_TB_LIST[] tbs = blaster.jni_traceback(hspl, partitionobj, requestobj, logLevel);
            System.out.println("traceback done");
            System.out.println("--- TRACEBACK RESULTS ---");

            if (tbs != null) {
                for (BLAST_TB_LIST tb : tbs) {
                    // System.out.println(tb.toString());
                }

                if (tbs.length > 0) {
                    byte[][] oneasn = new byte[1][0];
                    oneasn[0] = tbs[0].asn1_blob;
                    System.out.println("Dumped " + requestobj.id + " to file.");
                }
            } else {
                System.out.println("NULL asn1");
            }
            long finishtime = System.currentTimeMillis();
            System.out.println(
                    "Partition " + part_num + " finished traceback in " + (finishtime - middletime) + " ms");
            System.out.println(
                    "Partition " + part_num + " finished in " + (finishtime - starttime) + " ms");
            System.out.println("thread " + part_num + " finished.");
        } catch (Exception e) {
            System.err.println("prelim_Search or traceback exception");
        }
    }
}

class BLAST_BENCH {

    public static void doit_seq(int part_num, BLAST_REQUEST requestobj, BLAST_PARTITION partitionobj) {
        final String logLevel = "DEBUG";

        long starttime = System.currentTimeMillis();
        BLAST_LIB blaster = new BLAST_LIB();

        try {
            BLAST_HSP_LIST hspl[] = blaster.jni_prelim_search(partitionobj, requestobj, logLevel);
            if (hspl != null) {
                System.out.println(" prelim_search returned " + hspl.length + " HSP lists:");
                for (BLAST_HSP_LIST hsp : hspl) {
                    System.out.println(hsp.toString());
                }
            } else {
                System.out.println("NULL hspl");
            }
            long middletime = System.currentTimeMillis();
            System.out.println(
                    "Partition "
                    + part_num
                    + " finished prelim_search in "
                    + (middletime - starttime)
                    + " ms");

            BLAST_TB_LIST[] tbs = blaster.jni_traceback(hspl, partitionobj, requestobj, logLevel);

            System.out.println("traceback done");
            System.out.println("--- TRACEBACK RESULTS ---");

            if (tbs != null) {
                for (BLAST_TB_LIST tb : tbs) {
                    System.out.println(tb.toString());
                }

                if (tbs.length > 0) {
                    byte[][] oneasn = new byte[1][0];
                    oneasn[0] = tbs[0].asn1_blob;
                    System.out.println("Dumped " + requestobj.id + " to file.");
                }
            } else {
                System.out.println("NULL asn1");
            }
            long finishtime = System.currentTimeMillis();
            System.out.println(
                    "Partition " + part_num + " finished traceback in " + (finishtime - middletime) + " ms");
            System.out.println(
                    "Partition " + part_num + " finished in " + (finishtime - starttime) + " ms");
        } catch (Exception e) {
            System.err.println("prelim_Search or traceback exception");
        }
    }

    public static void main(String[] args) throws Exception, InterruptedException {
        final String rid = "ReqIDBench";
        //            "CCGCAAGCCAGAGCAACAGCTCTAACAAGCAGAAATTCTGACCAAACTGATCCGGTAAAACCGATCAACG";
        final String query_seq =
            "CCTTGCCGCGCCTAACCATGCAGTCGAACGGTAACAGAAAGCAGCTTGCTGCTTTGCTGACGAGTGGCGGACGGGTGAGTAATGTCTGGGAAACTGCCTGATGGAGGGGGATAACTACTGGAAACGGTAGCTAATACCGCATAACGTCGCAAGACCAAAGAGGGGGACCTTCGGGCCTCTTGCCATCGGATGTGCCCAGATGGGATTAGCTAGTAGGTGGGGTAAAGGCTCACCTAGGCGACGATCCCTAGCTGGTCTGAGAGGATGACCAGCCACACTGGAACTGAGACACGGTCCAGACTCCTACGGGAGGCAGCAGTGGGGAATATTGCACAATGGGCGCAAGCCTGATGCAGCCATGCCGCGTGTATGAAGAAGGCCTTCGGGTTGTAAAGTACTTTCAGCGGGGAGGAAGGGAGTAAAGTTAATACCTTTGCTCATTGACGTTACCCGCAGAAGAAGCACCGGCTAACTCCGTGCCAGCAGCCGCGGTAATACGGAGGGTGCAAGCGTTAATCGGAATTACTGGGCGTAAAGCGCACGCAGGCGGTTTGTTAAGTCAGATGTGAAATCCCCGGGCTCAACCTGGGAACTGCATCTGATACTGGCAAGCTTGAGTCTCGTAGAGGGGGGTAGAATTCCAGGTGTAGCGGTGAAATGCGTAGAGATCTGGAGGAATACCGGTGGCGAAGGCGGCCCCCTGGACGAAGACTGACGCTCAGGTGCGAAAGCGTGGGGGAGCAAACAGGATTAGATACCCTGGTAGTCCACGCCGTAAACGATGTCGACTTGGAGGTTGTGCCCTTGAGGCGTGGCTTCCGGAGCTAACGCGTTAAGTCGACCGCCTGGGGAGTACGGCCGCAAGGTTAAAACTCAAATGAATTGACGGGGGCCCGCACAGCGGTGGGAGCATGGTGGATTAATTCGATGCCACGCGAGATCTACTGGTCTGACATCCACGGAAGTTTTCGGAGATGGAAGAAATGTGTGG";
        final String query_url = ""; //"gs://blast-largequeries/query-021125518.txt";
        //final String location = "/tmp/vartanianmh/";
        // final String location = "/run/user/12953/";
        final String location="/tmp/blast/db";
        final String db_part = "nt_50M";
        final String program = "blastn";

        String params = "{";
        params += "\n\"version\": 1,";
        params += "\n \"RID\": \"" + rid + "\" ,";
        params += "\n \"blast_params\": { \"todo\": \"todo\" } }";
        params += "\n";

        final Integer top_n = 100;

        int mod = 0;
        if (args.length > 0) {
            mod = Integer.parseInt(args[0]);
        }

        BLAST_REQUEST requestobj = new BLAST_REQUEST();
        requestobj.id = rid;
        requestobj.query_seq = query_seq;
        requestobj.query_url = query_url;
        requestobj.params = params;
        requestobj.db = params;
        requestobj.program = program;
        requestobj.top_n = top_n;

        for (int part_num = 0; part_num != 886; ++part_num) {
            BLAST_PARTITION partitionobj = new BLAST_PARTITION(location, db_part, part_num, true); // false for GCP
            if (false) {
                doit_seq(part_num, requestobj, partitionobj);
            } else {
                if ((part_num % 10) != mod) continue;

                ConcDoit c = new ConcDoit(part_num, requestobj, partitionobj);
                c.start();
                while (java.lang.Thread.activeCount() >= 8) {
                    Thread.sleep(100);
                    System.out.println(java.lang.Thread.activeCount() + " active threads.");
                }
            }
        }
    }
}
