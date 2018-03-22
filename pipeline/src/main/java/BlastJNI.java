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
import java.lang.String;
import java.lang.System;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.nio.channels.FileLock;
import java.nio.channels.FileChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.storage.StorageOptions;

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

        // This requires 118MB of .jars, could also just invoke cache_files.sh
        // which would invoke gsutil
        if(!Files.exists(Paths.get(donefile))) {
            log(donefile + " doesn't exist. Checking if locked...");
            try {
                File dir=new File(dbdir);
                dir.mkdirs();

                File flock=new File(lockfile);
                FileChannel channel=new RandomAccessFile(flock, "rw").getChannel();
                FileLock lock=channel.lock();
                // ^^^ blocks
                if(!Files.exists(Paths.get(donefile))) {
                    log(donefile + " still doesn't exist. This thread will lock and download.");
                    Storage storage=StorageOptions.getDefaultInstance().getService();

                    log("Got storage for bucket " + db_bucket);

                    Bucket bucket=storage.get(db_bucket);
                    for(Blob blob : bucket.list(
                                Storage.BlobListOption.prefix(part + ".")).
                            iterateAll()) {
                        String dbfile=blob.getName();
                        String ext=dbfile.substring(dbfile.length() - 4);
                        if(ext.endsWith("sq") || ext.endsWith("in")) {
                            Path path=Paths.get(dbdir + db + ext);
                            log("    Downloading " + dbfile +
                                    " -> " + path + " ...");
                            blob.downloadTo(path);
                        } else {
                            log("    Skipping " + dbfile);
                        }
                            }

                    // Create donefile
                    File fdone=new File(donefile);
                    fdone.createNewFile();
                    log("Created " + donefile);

                    flock.delete();
                }

                if(lock != null) {
                    lock.release();
                }
                channel.close();
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

        String hsp[]={""};
        String[] tracebacks=new BlastJNI().jni_traceback(hsp);
    }
}

