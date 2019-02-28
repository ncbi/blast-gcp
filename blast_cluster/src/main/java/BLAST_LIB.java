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

import java.lang.management.ManagementFactory;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkFiles;

/**
 * wrapper for calling the jni-interface for BLAST
 * name and layout is coupled with the C++-code
 *
*/
public class BLAST_LIB
{

/**
 * constructor responsible for loading the library containing the jni- and BLAST-code
 *
 * @param libname	name of the library to look for
 *
*/
    public BLAST_LIB( final String libname )
    {
		// Guaranteed to be a singleton courtesy of BLAST_LIB_SINGLETON
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

		/*
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
		*/
        processID = ManagementFactory.getRuntimeMXBean().getName().split("@",2)[0];
        logLevel = Level.ERROR;
    }

    private static String processID;
    private static ExceptionInInitializerError invalid;
    private static Level logLevel;

/**
 * helper-method to throw an exception if invalid-exception is initialized
 *
 *
*/
    final void throwIfBad()
    {
        if ( invalid != null )
        {
            throw invalid;
        }
    }

/**
 * helper-method to write to log4j, filtered by log-level
 *
 * @param level		log-level to filter by
 * @param msg		message to log
 *
*/
    private void log( final String level, final String msg )
    {
    // We can't rely on log4j.properties to filter, instead we'll look at
    // logLevel
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


/**
 * helper-method
 *
 * @param url	url to test if it starts with 'gs://'
 * @return		empty string
 *
*/
    final String get_blob( String url )
    {
        if ( !url.startsWith( "gs://" ) )
        {
            log( "ERROR", "url: " + url + " not in a gs:// bucket" );
            return "";
        }
        return "";
    }

/**
 * wrapper around call to jni-interface 'prelim_search'
 *
 * @param chunk			database-chunk to search against
 * @param req			request to search for in the database-chunk
 * @param pslogLevel	level for logging
 * @return				vector of BLAST_HSP_LIST-instances
 *
*/
    final BLAST_HSP_LIST[] jni_prelim_search( final BC_DATABASE_RDD_ENTRY chunk,
			final BC_REQUEST req, final String pslogLevel ) throws Exception
	{
        // CMT - I hadn't intended this to be used to guard every method, but it's safer to do so
        throwIfBad();

		/*
        logLevel = Level.toLevel( pslogLevel );

        // CMT - remember that white space is good. Imagine it like a sort of cryptocurrency mining tool
        log( "INFO", "Java jni_prelim_search called with" );
        log( "INFO", "  query_seq : " + req.query_seq );
        log( "INFO", "  db_spec   : " + chunk.name );
        log( "INFO", "  program   : " + req.program );
        // FIX - top_n_prelim
        log( "INFO", "  topn      : " + req.top_n_prelim );

        if ( req.query_seq.contains( "\n" ) )
		{
            log( "WARN", "Query contains newline, which may crash Blast library" );
        }
		*/

        long starttime = System.currentTimeMillis();
        BLAST_HSP_LIST[] ret = prelim_search( req.query_seq, chunk.worker_location(), req.program, req.params, req.top_n_prelim );
        long finishtime = System.currentTimeMillis();

		/*
        log( "INFO", "jni_prelim_search returned in " + ( finishtime - starttime ) + " ms.");
        log( "INFO", "jni_prelim_search returned " + ret.length + " HSP_LISTs:" );
		*/

        int hspcnt = 0;
        for ( BLAST_HSP_LIST hspl : ret )
        {
            if ( hspl == null )
            {
                log( "ERROR", "hspl is null" );
                throw new Exception( "hspl " + hspcnt + " is null" );
            }
			/*
            if ( chunk == null )
				log( "ERROR", "chunk is null" );
            log( "DEBUG", "#" + hspcnt + ": " + hspl.toString() );
			*/
            ++hspcnt;
        }
        return ret;
    }

/**
 * wrapper around call to jni-interface 'traceback'
 *
 * @param hspl			vector of search-results
 * @param chunk			database-chunk to use
 * @param req			request to use
 * @param tblogLevel	level for logging
 * @return				vector of BLAST_TP_LIST-instances
 *
*/
    final BLAST_TB_LIST[] jni_traceback( final BLAST_HSP_LIST[] hspl, final BC_DATABASE_RDD_ENTRY chunk,
            final BC_REQUEST req, final String tblogLevel )
	{
        throwIfBad();

		/*
        logLevel = Level.toLevel( tblogLevel );
        log( "INFO", "Java jni_traceback called with" );
        log( "INFO", "  query_seq : " + req.query_seq );
        log( "INFO", "  db_spec   : " + chunk.name );
		*/

        long starttime = System.currentTimeMillis();
        BLAST_TB_LIST[] ret = traceback( hspl, req.query_seq, chunk.worker_location(), req.program, req.params );
        long finishtime = System.currentTimeMillis();

		/*
        log( "INFO", "jni_traceback returned in " + (finishtime - starttime) + " ms." );
        log( "INFO", "jni_traceback returned " + ret.length + " TB_LISTs:") ;
		*/

        for ( BLAST_TB_LIST t : ret )
        {
            t.top_n = req.top_n_traceback;
        }
        return ret;
    }

/**
 * jni-interface call to perform prelim_search, implemented in C++
 *
 * @param query			request-query
 * @param dbspec		path to database-chunk on local disk
 * @param program		'nt' or 'nr'
 * @param params		json-encoded search parameters
 * @param topn			after how many unique score-values to cut the result-vector
 * @return				vector of BLAST_TP_LIST-instances
 *
*/
    private native BLAST_HSP_LIST[] prelim_search(
            String query, String dbspec, String program, String params, int topn );

/**
 * jni-interface call to perform 'traceback', implemented in C++
 *
 * @param hspl			vector of search-results
 * @param query			request-query
 * @param dbspec		path to database-chunk on local disk 
 * @param program		'nt' or 'nr'
 * @param params		json-encoded search parameters
 * @return				vector of BLAST_TP_LIST-instances
 *
*/
    private native BLAST_TB_LIST[] traceback(
            BLAST_HSP_LIST[] hspl, String query, String dbspec, String program, String params );
}
