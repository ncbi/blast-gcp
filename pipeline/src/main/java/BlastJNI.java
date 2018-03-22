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

import java.util.Arrays;
import java.util.ArrayList;
import java.lang.ProcessBuilder;
import java.lang.String;
import java.lang.System;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileNotFoundException;
import java.nio.file.attribute.PosixFilePermission;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.nio.channels.FileLock;
import java.nio.channels.FileChannel;
import org.apache.spark.*;
import org.apache.spark.SparkFiles;

public class BlastJNI {
    static {
        try {
            // Java will look for libblastjni.so
            System.loadLibrary("blastjni");
            //System.load(SparkFiles.get("libblastjni.so"));
        } catch(Exception e) {
            try {
                log("Couldn't System.loadLibrary, trying System.load");
                System.load(SparkFiles.get("libblastjni.so"));
            } catch(Exception e2) {
                log("System.load() exception: " + e2);
            }
        }
    }

    private native String[] prelim_search(String dbenv, String rid, String query, String db_part, String params);

    private native String[] traceback(String[] jsonHSPs);

    public static void log(String msg) {
        try {
            System.out.println(msg);
            PrintWriter pw=new PrintWriter(new FileOutputStream(new File("/tmp/blastjni.log"), true));
            pw.println("(java) "+ msg);
            pw.close();
        } catch(FileNotFoundException ex) {
        }
    }

    private String cache_dbs(String db_bucket, String db, String part) {
        log("cache_dbs(db_bucket=" + db_bucket + ", db=" + db + ", part=" + part + ")");

        String localdir="/tmp/blast/";
        String dbdir=localdir + part + "/";
        String donefile=dbdir + "done";
        String lockfile=dbdir + "lock";

        if(!Files.exists(Paths.get(donefile))) {
            try {
            log(donefile + " doesn't exist.");

            File dir=new File(dbdir);
            dir.mkdirs();

            File scriptfile=File.createTempFile("blastjni-cache-",".sh");
            scriptfile.deleteOnExit();
            String scriptname=scriptfile.getAbsolutePath();
            log("Creating shell script:" + scriptname);
            PrintWriter pw=new PrintWriter(new FileOutputStream(scriptfile, true));
            pw.println("#!/usr/bin/env bash");
            pw.println("BUCKET=$1");
            pw.println("PART=$2");
            pw.println("DIR=$3");
            pw.println("LOCKFILE=$4");
            pw.println("DONEFILE=$5");
            pw.println("cd $DIR");
            pw.println("exec >  >(tee -ia log)");
            pw.println("exec 2> >(tee -ia log) >&2");
            pw.println("date");
            pw.println("echo \"BUCKET is   $BUCKET\" ");
            pw.println("echo \"PART is     $PART\" ");
            pw.println("echo \"DIR is      $DIR\" ");
            pw.println("echo \"LOCKFILE is $LOCKFILE\" ");
            pw.println("echo \"DONEFILE is $DONEFILE\" ");
            pw.println("echo \"attempting to lock (pid=$$)\"");
            pw.println("#lockfile $LOCKFILE # blocks");
            pw.println("if [ -e $DONEFILE ]; then");
            pw.println("  echo \"already done (pid=$$)\"");
            pw.println("  exit 0 # Another thread completed this.");
            pw.println("fi");
            pw.println("#gsutil ls -l gs://$BUCKET/$PART* ");
            pw.println("#gsutil -m cp gs://$BUCKET/$PART .");
            pw.println("#gsutil -m cp \"gs://$BUCKET/$PART.*\" .");
            pw.println("gsutil -m cp \"gs://$BUCKET/$PART.*in\" .");
            pw.println("gsutil -m cp \"gs://$BUCKET/$PART.*sq\" .");
            pw.println("touch $DONEFILE");
            pw.println("rm -f $LOCKFILE");
            pw.println("date");
            pw.println("exit 0");
            pw.close();
            scriptfile.setExecutable(true,true);

            ProcessBuilder pb=new ProcessBuilder(scriptname,
                    db_bucket,
                    part,
                    dbdir,
                    lockfile,
                    donefile);
            Process p=pb.start();

            } catch(Exception e) {
                log("exception in cache method: " + e);
            }
        } else
        {
            log(dbdir + " cached.");
        }

        return dbdir;
    }

    //private native String[] traceback(String[] jsonHSPs);
    public String[] jni_traceback(String[] jsonHSPs)
    {
        log("jni_traceback called with ");
        for (String s: jsonHSPs)
        {
            log("hsp:" + s);
        }

        //String dbenv=cache_dbs(db_bucket, db, part);

        String[] results=traceback(jsonHSPs);
        log("jni_traceback returned " + results.length + " results:");
        for (String s: results)
        {
            log("tb:" + s);
        }

        return results;
    }

    public String[] jni_prelim_search(String db_bucket, String db, String rid, String query, String part, String params) {
        log("jni_prelim_search called with " + db_bucket + "," + db + "," + rid + "," + query + "," + part + "," + params);
        String dbenv=cache_dbs(db_bucket, db, part);

        String[] results=prelim_search(dbenv, rid, query, db, params);
        log("jni_prelim_search returned " + results.length + " results");
        return results;
    }


    public static void main(String[] args) {
        String rid  ="ReqID123";
        String query ="CCGCAAGCCAGAGCAACAGCTCTAACAAGCAGAAATTCTGACCAAACTGATCCGGTAAAACCGATCAACG";
        String db    ="nt";
        String db_bucket=db + "_50mb_chunks";
        String params="blastn";

        ArrayList<String> al=new ArrayList<String>();
        for (int partnum=0; partnum <= 26; ++partnum)
        {
            String part=db + "_50M." + String.format("%02d", partnum);

            String results[]=new BlastJNI().jni_prelim_search(db_bucket, db, rid, query, part, params);

            log("Java results[] has " + results.length + " entries:");
            log(Arrays.toString(results));
            al.addAll(Arrays.asList(results));
        }

        String hsp[]=al.toArray(new String[0]);
        String[] tracebacks=new BlastJNI().jni_traceback(hsp);
    }
}

