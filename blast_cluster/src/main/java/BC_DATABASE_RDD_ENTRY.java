/*===========================================================================
*
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
*
* ===========================================================================
*
*/

package gov.nih.nlm.ncbi.blastjni;

import java.io.Serializable;
import java.io.File;

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

import java.net.InetAddress;

import org.apache.spark.SparkEnv;

/**
 * Content of the Database-RDD
 * - stores a reference to database-settings common to all chunks
 * - stores the name of one specific database-chunk. for instance : 'nr_50M.00'
 *
 * @see        BC_DATABASE_SETTING
*/
public class BC_DATABASE_RDD_ENTRY implements Serializable
{
    private final BC_DATABASE_SETTING setting;
    public final String name;

/**
 * create instance BC_DATABASE_RDD_ENTRY
 * - store common database-settings and name
 *
 * @param a_setting  common database-settings
 * @param a_name     name of one particular database-chunk
 * @see        BC_DATABASE_SETTING
*/
    public BC_DATABASE_RDD_ENTRY( final BC_DATABASE_SETTING a_setting, final String a_name )
    {
        setting = a_setting;
        name = a_name;
    }

/**
 * static method to create a list of BC_DATABASE_RDD_ENTRY-instances
 *
 * @param a_setting  common database-settings
 * @param names      list of database-chunk-names
 * @return           a list of BC_DATABASE_RDD_ENTRY-instances
 * @see              BC_DATABASE_SETTING
*/
    public static List< BC_DATABASE_RDD_ENTRY > make_rdd_entry_list( final BC_DATABASE_SETTING a_setting, final List< String > names )
    {
        List< BC_DATABASE_RDD_ENTRY > res = new ArrayList<>();
        for ( String a_name : names )
            res.add( new BC_DATABASE_RDD_ENTRY( a_setting, a_name ) );
        return res;
    }

/**
 * construct the source-path for one of the database-chunk-files based on its extension
 *
 * @param extension  one of the extensions of database-chunk, for instance 'nhr'
 * @return           fully qualified url of one of the files of a datbase-chunk
 * @see              BC_DATABASE_SETTING
*/
    public String build_source_path( final String extension )
    {
        return String.format( "%s/%s.%s", setting.source_location, name, extension );
    }

/**
 * construct the destination-path one of the database-chunk-files based on its extension
 *
 * @param extension  one of the extensions of database-chunk, for instance 'nhr'
 * @return           absolute path of one of the files of a datbase-chunk on the worker
 * @see              BC_DATABASE_SETTING
*/
    public String build_worker_path( final String extension )
    {
        return String.format( "%s/%s/%s.%s", setting.worker_location, name, name, extension );
    }

/**
 * construct the absolute path of the directory where the 3 database-chunk files are
 * to be found, for instance '/tmp/blast/db/nt_50M.00'
 *
 * @return           absolute path of one of the datbase-chunks on the worker
 * @see              BC_DATABASE_SETTING
*/
    public String worker_location()
    {
        return String.format( "%s/%s/%s", setting.worker_location, name, name );
    }

/**
 * construct the name of the worker ( DNS-name + executor-id ) for debug purpose
 *
 * @return           worker-name, for instance 'cluster-w-0/1'
*/
    public String workername()
    {
        String w;
        try { w = java.net.InetAddress.getLocalHost().getHostName(); }
        catch( Exception e ) { w = "?"; }
        return String.format( "%s/%s", w, SparkEnv.get().executorId() );
    }

/**
 * check if all files of a database-chunk are present on the worker
 *
 * @return           are all files of a database-chunk present on the worker
 * @see              BC_DATABASE_SETTING
*/
    public boolean present()
    {
        int found = 0;
        for ( String extension : setting.extensions )
        {
            File f_dst = new File( build_worker_path( extension ) );
            if ( f_dst.exists() )
                found += 1;
        }
        return ( found == setting.extensions.size() );
    }

/**
 * download ( if neccessary ) all files fo a database-chunk to the worker
 *
 * @return           list of strings documenting the download process
 * @see              BC_DATABASE_SETTING
 * @see              BC_GCP_TOOLS
*/
    public List< String > download()
    {
        List< String > lst = new ArrayList<>();
        String wn = workername();
        for ( String extension : setting.extensions )
        {
            String src = build_source_path( extension );
            String dst = build_worker_path( extension );
            File f_dst = new File( dst );
            if ( f_dst.exists() )
                lst.add( String.format( "%s : %s -> %s (exists)", wn, src, dst ) );
            else
            {
                long started_at = System.currentTimeMillis();
                boolean success = BC_GCP_TOOLS.download( src, dst );
                long elapsed = System.currentTimeMillis() - started_at;
                lst.add( String.format( "%s : %s -> %s (%s in %,d ms)", wn, src, dst, Boolean.toString( success ), elapsed ) );
            }
        }
        return lst;
    }
}

