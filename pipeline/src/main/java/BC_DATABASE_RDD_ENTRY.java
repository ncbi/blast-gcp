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
import java.io.FileOutputStream;
import java.io.FileNotFoundException;

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

import java.net.InetAddress;

import org.apache.spark.SparkEnv;

import java.nio.channels.FileLock;

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
    public final BC_CHUNK_VALUES chunk;
    private static Object mutex = new Object();
    private final byte[] ballast;

/**
 * create instance BC_DATABASE_RDD_ENTRY
 * - store common database-settings and name
 *
 * @param a_setting  common database-settings
 * @param a_name     name of one particular database-chunk
 * @see        BC_DATABASE_SETTING
*/
    public BC_DATABASE_RDD_ENTRY( final BC_DATABASE_SETTING a_setting, final BC_CHUNK_VALUES a_chunk )
    {
        setting = a_setting;
        chunk = a_chunk;
        if ( setting.ballast > 0 )
            ballast = new byte[ setting.ballast ];
        else
            ballast = null;
    }

/**
 * static method to create a list of BC_DATABASE_RDD_ENTRY-instances
 *
 * @param a_setting  common database-settings
 * @param names      list of database-chunk-names
 * @return           a list of BC_DATABASE_RDD_ENTRY-instances
 * @see              BC_DATABASE_SETTING
*/
    public static List< BC_DATABASE_RDD_ENTRY > make_rdd_entry_list( final BC_DATABASE_SETTING a_setting,
            final List< BC_CHUNK_VALUES > files )
    {
        List< BC_DATABASE_RDD_ENTRY > res = new ArrayList<>();
        for ( BC_CHUNK_VALUES chunk : files )
            res.add( new BC_DATABASE_RDD_ENTRY( a_setting, chunk ) );
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
        return String.format( "%s/%s.%s", setting.source_location, chunk.name, extension );
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
        return String.format( "%s/%s/%s.%s", setting.worker_location, chunk.name, chunk.name, extension );
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
        if ( setting.direct )
            return String.format( "%s/%s", setting.worker_location, chunk.name );
        else
            return String.format( "%s/%s/%s", setting.worker_location, chunk.name, chunk.name );
    }

/**
 * construct the name of the worker ( DNS-name + executor-id ) for debug purpose
 *
 * @return           worker-name, for instance 'cluster-w-0/1'
*/
    public String workername()
    {
        String w;

        try {
            w = java.net.InetAddress.getLocalHost().getHostName();
        }
        catch( Exception e ) { w = "?"; }

        try {
            return String.format( "%s/%s", w, SparkEnv.get().executorId() );
        }
        catch (Exception e) // Running outside Spark
        {
            return String.format( "%s/localhost", w);
        }
        catch (NoClassDefFoundError e)
        {
            return String.format( "%s/localhost", w);
        }
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
        for ( BC_NAME_SIZE obj : chunk.files )
        {
            String extension = obj.name;
            File f = new File( build_worker_path( extension ) );
            if ( f.exists() )
            {
                /* new: we can now check for the correct size */
                if ( obj.size.longValue() == f.length() )
                    found += 1;
            }
        }
        return ( found == chunk.files.size() );
    }

/**
 * download ( if neccessary ) all files fo a database-chunk to the worker
 *
 * @param       error_lst       list of download-errors
 * @param       info_lst        list of info's
 * @return      success
 *
 * @see              BC_DATABASE_SETTING
 * @see              BC_GCP_TOOLS
*/
    public boolean download( List< String > error_lst, List< String > info_lst )
    {
        String wn = workername();
        for ( BC_NAME_SIZE obj : chunk.files )
        {
            String extension = obj.name;
            String src = build_source_path( extension );
            String dst = build_worker_path( extension );
            File f = new File( dst );
            if ( f.exists() )
            {
                long fl = f.length();
                /* we can now check the size... */
                if ( obj.size.longValue() == fl )
                    info_lst.add( String.format( "%s : %s -> %s ( exists size = %d )", wn, src, dst, fl ) );
                else
                    info_lst.add( String.format( "%s : %s -> %s ( exists, size=%d, should be %d )", wn, src, dst, fl, obj.size ) );
                    /* this is not an error, the caller will retry in this case */
            }
            else
            {
                long started_at = System.currentTimeMillis();
                boolean success = BC_GCP_TOOLS.download( src, dst );
                long elapsed = System.currentTimeMillis() - started_at;

                long fl = f.length();
                if ( success )
                {
                    /* we can now check the size... */
                    if ( obj.size.longValue() == fl )
                        info_lst.add( String.format( "%s : %s -> %s ( OK in %,d ms, size=%d )", wn, src, dst, elapsed, fl ) );
                    else
                    {
                        error_lst.add( String.format( "%s : %s -> %s ( SIZE-ERROR in %,d ms, size=%d, should=%d )", wn, src, dst, elapsed, fl, obj.size ) );
                        return false;
                    }
                }
                else
                {
                    error_lst.add( String.format( "%s : %s -> %s ( FAILED in %,d ms, size=%d, should=%d )", wn, src, dst, elapsed, fl, obj.size ) );
                    return false;
                }
            }
        }
        return true;
    }



/**
 * Check a database file is present and download if it is not. The method is
 * synchronized for threads and processes. Check and download is atomic.
 *
 * @param       report, list of string reporting the download
 * @return      number of errors
 * @see              BC_DATABASE_SETTING
 * @see              BC_GCP_TOOLS
*/
    public boolean downloadIfAbsent(List<String> error_lst,
                                    List<String> info_lst)
    {
        String wn = workername();

        for ( BC_NAME_SIZE obj : chunk.files )
        {
            String extension = obj.name;
            String src = build_source_path( extension );
            String dst = build_worker_path( extension );
            File f = new File( dst );

            String parent = f.getParent();
            if (parent != null) {
                File p = new File(parent);
                p.mkdirs();
            }

            File ff = new File( dst + ".lock");
            FileOutputStream f_out = null;
            FileLock f_lock = null;

            // synchronize threads within a jvm
            synchronized(mutex) {

                try {

                    f_out = new FileOutputStream( ff );

                    // synchronize jvms
                    f_lock = f_out.getChannel().lock();

                    if ( f.exists() ) {
                        long fl = f.length();
                        /* we can now check the size... */
                        if ( obj.size.longValue() == fl ) {
                            info_lst.add( String.format(
                                          "%s : %s -> %s (exists size = %d )",
                                          wn, src, dst, fl ) );
                        }
                        else {
                            error_lst.add( String.format(
                               "%s : %s -> %s (exists, size=%d, should be %d)",
                               wn, src, dst, fl, obj.size ) );
                        }
                    }
                    else {
                        long started_at = System.currentTimeMillis();
                        boolean success = BC_GCP_TOOLS.download( src, dst );
                        long elapsed = System.currentTimeMillis() - started_at;

                        /* we can now check the size... */
                        long fl = f.length();
                        if (success) {
                            if ( obj.size.longValue() == fl ) {
                                info_lst.add( String.format(
                                     "%s : %s -> %s (%s in %,d ms, size=%d)",
                                     wn, src, dst, Boolean.toString( success ),
                                     elapsed, fl ) );
                            }
                            else {
                                success = false;
                                error_lst.add( String.format(
                                    "%s : %s -> %s ( SIZE-ERROR in %,d ms, size=%d, should=%d )",
                                    wn, src, dst, elapsed, fl, obj.size ) );
                                return false;
                            }
                        }
                        else {
                            error_lst.add( String.format(
                                 "%s : %s -> %s ( FAILED in %,d ms, size=%d, should=%d )",
                                 wn, src, dst, elapsed, fl, obj.size ) );
                    return false;

                        }
                    }

                }
                catch (java.io.FileNotFoundException e) {
                    e.printStackTrace();
                }
                catch (java.io.IOException e) {
                    e.printStackTrace();
                }
                finally {
                    try {
                        f_lock.release();
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        return true;
    }

}
