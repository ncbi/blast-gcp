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

import java.util.Arrays;
import java.lang.String;
import java.lang.System;
import java.io.*;

public class BlastJNI {
    static {
        //System.loadLibrary("blastjni");
        System.load("/tmp/blast/blastjni.so");
    }

    private static void log(String msg) {
        try {
            PrintWriter pw = new PrintWriter(new FileOutputStream(new File("/tmp/blastjni.java.log"),true));
            pw.println(msg);
            pw.close();
        } catch (FileNotFoundException ex)
        {}
    }

    private native String[] prelim_search(String rid, String query, String db_part, String params);

    public String[] jni_prelim_search(String rid, String query, String db_part, String params)
    {
        log("Java jni_prelim_search called with:");
        log("                               id=" + rid);
        log("                            query=" + query);
        log("                          db_part=" + db_part);
        log("                           params=" + params);
        String[] results=prelim_search(rid, query, db_part, params);
        log("jni_prelim_search returned " + results.length + " results");
        return results;
    }

    public static void main(String[] args) {
        String results[] = new BlastJNI().jni_prelim_search("123","CCGCAAGCCAGAGCAACAGCTCTAACAAGCAGAAATTCTGACCAAACTGATCCGGTAAAACCGATCAACG","nt.04","blastn");
        System.out.println("Java results[] has "+ results.length + " entries:");
        System.out.println(Arrays.toString(results));
    }
}

