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
import java.io.*;

import org.apache.spark.*;
import org.apache.spark.SparkFiles;

public class BlastJNI
{
    static
    {
        try
        {
            System.load( SparkFiles.get( "libblastjni.so" ) );
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

    private native String[] prelim_search( String jobid, String query, String db_part, String params );

    public String[] jni_prelim_search( String jobid, String query, String db_part, String params )
    {
        log( "jni_prelim_search called with "+query+","+db_part+","+params );
        String[] results = prelim_search( jobid, query, db_part, params );
        log( "jni_prelim_search returned " + results.length + " results" );
        return results;
    }

    public static void main( String[] args )
    {
        String r_id   = "ReqID123";
        String query  = "CCGCAAGCCAGAGCAACAGCTCTAACAAGCAGAAATTCTGACCAAACTGATCCGGTAAAACCGATCAACG";
        String db     = "nt.04";
        String params = "blastn";
        
        String results[] = new BlastJNI().jni_prelim_search( r_id, query, db, params );
        
        System.out.println( "Java results[] has " + results.length + " entries:" );
        System.out.println( Arrays.toString( results ) );
    }
}
