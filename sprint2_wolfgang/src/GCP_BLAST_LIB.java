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

import java.lang.String;
import java.lang.System;
import java.io.PrintWriter;
import org.apache.spark.SparkFiles;

class GCP_BLAST_LIB
{
    String [] jni_prelim_search ( String query, String db_spec, String program, String params )
    {
        if ( log_writer != null )
            log ( "jni_prelim_search called with " + query + ",'" + db_spec + "','" + program + "," + params + "'" );

        String [] results = prelim_search ( query, db_spec, program, params );

        if ( log_writer != null )
            log ( "jni_prelim_search returned " + results . length + " results" );
        
        return results;
    }

    String [] jni_traceback ( String query, String db_spec, String program, String params, final String [] jsonHSPs )
    {
        if ( log_writer != null )
        {
            log ( "jni_traceback called with:" );
            for ( String s: jsonHSPs )
                log ( "hsp: " + s );
        }

        String [] results = traceback ( query, db_spec, program, params, jsonHSPs );

        if ( log_writer != null )
        {
            log ( "jni_traceback returned " + result . length + " result:" );
            for ( String s : results )
                log ( "traceback: " + s );
        }

        return results;
    }
    
    void setLogWriter ( PrintWriter writer )
    {
        log_writer = writer;
    }

    private synchronized void log ( String msg )
    {
        log_writer . println ( "(java) " + msg );
        log_writer . flush ();
    }
    
    GCP_BLAST_LIB ()
    {
        try
        {
            // Java will look for libblastjni.so
            System.loadLibrary( "blastjni" );
        }
        catch ( Throwable e )
        {
            try
            {
                System.load( SparkFiles.get( "libblastjni.so" ) );
            }
            catch ( ExceptionInInitializerError x )
            {
                invalid = x;
            }
            catch ( Throwable e2 )
            {
                invalid = new ExceptionInInitializerError ( e2 );
            }
        }
    }

    void throwIfBad ()
    {
        if ( invalid != null )
            throw invalid;
    }

    private ExceptionInInitializerError invalid;
    private PrintWriter log_writer;

    /*
    private native String [] prelim_search ( String query, String db_spec, String program, String params );
    private native String [] traceback ( String query, String db_spec, String program, String params, final String [] jsonHSPs );
    */

    String [] prelim_search ( String query, String db_spec, String program, String params )
    {
        String [] hsps = new String [ 1 ];
        hsps [ 0 ] = "fake HSP";
        return hsps;
    }

    String [] traceback ( String query, String db_spec, String program, String params, final String [] jsonHSPs )
    {
        String [] hsps = new String [ 1 ];
        hsps [ 0 ] = "fake traceback HSP";
        return hsps;
    }
}

