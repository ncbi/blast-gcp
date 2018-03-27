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
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.file.attribute.PosixFilePermission;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.nio.channels.FileLock;
import java.nio.channels.FileChannel;

public class BlastJNI {
    static
    {
        try
        {
            // Java will look for libblastjni.so
            System.loadLibrary( "blastjni" );
        }
        catch ( Exception e )
        {
            log( "System.load() exception: " + e );
        }
    }

    private native String[] prelim_search( String dbenv, String rid, String query, String db_part, String params );

    public String[] jni_prelim_search( String rid, String query, String part, String params )
    {
        System.out.println( String.format( "jni_prelim_search( rid:'%s', query:'%s', part:'%s', params:'%s'", rid, query, part, params ) );

        String[] results=prelim_search( "", rid, query, part, params );
        
        System.out.println( "jni_prelim_search returned " + results.length + " results" );

        return results;
    }


    public static void main( String[] args )
    {
        final String username = System.getProperty( "user.name" );        
        
        String rid    = "ReqID123";
        String query  = "CCGCAAGCCAGAGCAACAGCTCTAACAAGCAGAAATTCTGACCAAACTGATCCGGTAAAACCGATCAACG";
        String part   = String.format( "/home/%s/spark/db", usernam ) ;
        String params = "blastn";

        if ( args.length > 0 )
        {
            String[] req = args[ 1 ].split( "\\:" );
            if ( req.length > 0 ) part = req[ 0 ];
            if ( req.length > 1 ) rid  = req[ 1 ];
            if ( req.length > 2 ) query = req[ 2 ];
            if ( req.length > 3 ) params = req[ 3 ];
        }
        
        String S = String.format( "partition ... %s\n", part );
        S  =  S +  String.format( "req-id ...... %s\n", rid );
        S  =  S +  String.format( "query ....... %s\n", query );
        S  =  S +  String.format( "params ...... %s\n", params );
        System.out.println( S );
        
        BlastJNI blaster = new BlastJNI();
        String results[] = blaster.jni_prelim_search( rid, query, part, params );
    }
}

