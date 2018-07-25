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

/*
   import com.google.cloud.storage.Blob;
   import com.google.cloud.storage.BlobId;
   import com.google.cloud.storage.Storage;
   import com.google.cloud.storage.StorageOptions;
   */
import java.lang.management.ManagementFactory;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkFiles;

public class BLAST_LIB
{
    // Guaranteed to be a singleton courtesy of BLAST_LIB_SINGLETON
    public BLAST_LIB( final String libname )
    {
        try
        {
            // Java will look for libblastjni.so, thread safe if successful?
            final int lastPeriodPos = libname.lastIndexOf( '.' );
            if ( lastPeriodPos <= 0 )
                System.loadLibrary( libname );
            else
                System.loadLibrary( libname.substring( 0, lastPeriodPos ) );
        }
        catch ( Throwable threx )
        {
            try
            {
                System.load( SparkFiles.get( libname ) );
            }
            catch ( ExceptionInInitializerError xininit )
            {
                invalid = xininit;
            }
            catch ( Throwable threx2 )
            {
                invalid = new ExceptionInInitializerError( threx2 );
            }
        }
        processID = ManagementFactory.getRuntimeMXBean().getName().split("@",2)[0];
        logLevel = Level.INFO;
    }

    private static String processID;
    private static ExceptionInInitializerError invalid;
    private static Level logLevel;

    final void throwIfBad()
    {
        if ( invalid != null )
        {
            throw invalid;
        }
    }

    // We can't rely on log4j.properties to filter, instead we'll look at
    // logLevel
    public void log( final String level, final String msg )
    {
        try
        {
            Logger logger = LogManager.getLogger( BLAST_LIB.class );
            Level lvl = Level.toLevel( level );
            long threadId = Thread.currentThread().getId();

            if ( lvl.isGreaterOrEqual( logLevel ) )
            {
                final String newmsg = "BLASTJNI (" + BLAST_LIB.processID + "/" + threadId + ") " + msg;
                logger.log( lvl, newmsg );
            }
        }
        catch ( Throwable threx )
        {
            {
                System.err.println( "ERROR Log throw" );
                if ( msg != null )
                    System.err.println( msg );
            }
        }
    }

    final String get_blob( String url )
    {
        if ( !url.startsWith( "gs://" ) )
        {
            log( "ERROR", "url: " + url + " not in a gs:// bucket" );
            return "";
        }
        return "";
        /*

        // url is gs://foo/bar
        String path = url.substring(5); // foo/bar
        String bucketName = path.substring(0, path.indexOf("/", 1)); // foo
        String object = path.substring(path.indexOf("/", 1) + 1); // bar
        log("INFO", "path is " + path);
        log("INFO", "bucket is " + bucketName);
        log("INFO", "object is " + object);

        Storage storage = StorageOptions.getDefaultInstance().getService();
        BlobId blobId = BlobId.of(bucketName, object);
        byte[] content = storage.readAllBytes(blobId);
        Blob blob = storage.get(blobId);
        if (blob == null) {
        log("ERROR", "blob " + url + " not found");
        return "";
        }
        String res = "";
        byte[] content = blob.getContent();
        if (content.length == 0) {
        log("WARN", "empty blob for " + url);
        return "";
        }
        try {
        res = new String(content, "UTF-8");
        } catch (java.io.UnsupportedEncodingException uee) {
        log("ERROR", "blob isn't UTF-8");
        res = "";
        // FIX - Throw
        }
        return res;
        */
    }

    final BLAST_HSP_LIST[] jni_prelim_search(
            final BLAST_DATABASE_PART part, final BLAST_REQUEST req, final String pslogLevel)
            throws Exception {

            // CMT - I hadn't intended this to be used to guard every method, but it's safer to do so
            throwIfBad();

            BLAST_LIB.logLevel = Level.toLevel(pslogLevel);

            // CMT - remember that white space is good. Imagine it like a sort of cryptocurrency mining tool
            log( "INFO", "Java jni_prelim_search called with" );
            log( "INFO", "  query_seq : " + req.query_seq );
            log( "INFO", "  db_spec   : " + part.db_spec );
            log( "INFO", "  program   : " + req.program );
            // FIX - top_n_prelim
            log( "INFO", "  topn      : " + req.top_n_prelim );

            if (req.query_seq.contains("\n")) {
                log("WARN", "Query contains newline, which may crash Blast library");
            }

            String query;
            long starttime = System.currentTimeMillis();
            query = req.query_seq;

            BLAST_HSP_LIST[] ret = prelim_search( query, part.db_spec, req.program, req.params, req.top_n_prelim );

            long finishtime = System.currentTimeMillis();
            log("INFO", "jni_prelim_search returned in " + (finishtime - starttime) + " ms.");
            log("INFO", "jni_prelim_search returned " + ret.length + " HSP_LISTs:");
            int hspcnt = 0;
            for ( BLAST_HSP_LIST hspl : ret )
            {
                if ( hspl == null )
                {
                    log( "ERROR", "hspl is null" );
                    throw new Exception("hspl " + hspcnt + " is null");
                    //                continue;
                }
                if ( part == null ) log( "ERROR", "part is null" );
                //hspl.part = part;
                //hspl.req = req;
                log("DEBUG", "#" + hspcnt + ": " + hspl.toString());
                ++hspcnt;
            }

            return ret;
            }

    final BLAST_TB_LIST[] jni_traceback(
            final BLAST_HSP_LIST[] hspl,
            final BLAST_DATABASE_PART part,
            final BLAST_REQUEST req,
            final String tblogLevel) {
        throwIfBad();

        BLAST_LIB.logLevel = Level.toLevel( tblogLevel );
        log( "INFO", "Java jni_traceback called with" );
        log( "INFO", "  query_seq : " + req.query_seq );
        log( "INFO", "  db_spec   : " + part.db_spec );

        String query;
        long starttime = System.currentTimeMillis();
        query = req.query_seq;

        BLAST_TB_LIST[] ret = traceback( hspl, query, part.db_spec, req.program, req.params );
        long finishtime = System.currentTimeMillis();

        log("INFO", "jni_traceback returned in " + (finishtime - starttime) + " ms.");
        log("INFO", "jni_traceback returned " + ret.length + " TB_LISTs:");

        for ( BLAST_TB_LIST t : ret )
        {
            t.top_n = req.top_n_traceback;
        }

        return ret;
        }

    private native BLAST_HSP_LIST[] prelim_search(
            String query, String dbspec, String program, String params, int topn);

    private native BLAST_TB_LIST[] traceback(
            BLAST_HSP_LIST[] hspl, String query, String dbspec, String program, String params);
}
