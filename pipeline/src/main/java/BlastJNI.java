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
import com.google.cloud.storage.StorageOptions;

//import org.apache.spark.*;
//import org.apache.spark.SparkFiles;

public class BlastJNI
{
    static
    {
        try
        {
            // Java will look for libblastjni.so
            System.loadLibrary("blastjni"); 
            //System.load( SparkFiles.get( "libblastjni.so" ) );
        }
        catch ( Exception e )
        {
            System.out.println( "System.load() exception: " + e );
        }
    }

    public static void log( String msg )
    {
        try
        {
            PrintWriter pw = new PrintWriter( new FileOutputStream( new File( "/tmp/blastjni.java.log" ), true ) );
            pw.println( msg );
            pw.close();
        } catch ( FileNotFoundException ex )
        {}
    }

    private native String[] prelim_search(String dbenv, String rid, String query, String db_part, String params);

    private String cache(String db_bucket, String db, String db_part)
    {
        String localdir="/tmp/blast/";
        String dbdir=localdir+db+"/";
        String donefile=dbdir+"done";
        String lockfile=dbdir+"lock";

        if (!Files.exists(Paths.get(donefile)))
        {
            try {
                File dir=new File(dbdir);
                dir.mkdirs();

                File flock=new File(lockfile);
                FileChannel channel=new RandomAccessFile(flock,"rw").getChannel();
                FileLock lock=channel.lock();
                // ^^^ blocks
                if (!Files.exists(Paths.get(donefile)))
                {
                    // Done file still doesn't exist, this thread has to do the work
                    // gs://nt_500mb_chunks/nt_500M.57.nsq
                    Storage storage = StorageOptions.getDefaultInstance().getService();

                    System.out.println("Got storage");

                    Bucket bucket=storage.get(db_bucket);
                    for (Blob blob:bucket.list().iterateAll())
                    {
                        log("Downloading " + blob.getName());
                        //Blob blob=blobIterator.next();
                        Path path=Paths.get(localdir+blob.getName());

                        blob.downloadTo(path);
                    }

                    // Create donefile
                    File fdone =new File (donefile);
                    fdone.createNewFile();
                }

                if (lock!=null)
                {
                    lock.release();
                }
                channel.close();
            }
            catch ( Exception e )
            {
                System.out.println( "exception in cache method: " + e );
            }
        }

        return dbdir;
    }

    public String[] jni_prelim_search(String db_bucket, String db, String rid, String query, String db_part, String params )
    {
        log( "jni_prelim_search called with "+db_bucket+","+db+","+rid+","+ query+","+db_part+","+params );
        /* TODO: Cache */
        String dbenv=cache(db_bucket, db, db_part);

        String[] results = prelim_search( dbenv, rid, query, db, params );
        log( "jni_prelim_search returned " + results.length + " results" );
        return results;
    }

    public static void main( String[] args )
    {
        String rid   = "ReqID123";
        String query  = "CCGCAAGCCAGAGCAACAGCTCTAACAAGCAGAAATTCTGACCAAACTGATCCGGTAAAACCGATCAACG";
        // gs://nt_500mb_chunks/nt_500M.57.nsq
        String db     = "nt";
        String db_bucket = "gs://" + db + "_500mb_chunks/";
        String db_part=db+ "_500M." + "57";
        String params = "blastn";

        String results[] = new BlastJNI().jni_prelim_search( db_bucket, db, rid, query, db_part, params );

        System.out.println( "Java results[] has " + results.length + " entries:" );
        System.out.println( Arrays.toString( results ) );
    }
}
