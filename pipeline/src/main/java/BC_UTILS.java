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

import java.io.File;
import java.io.FileWriter;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.BufferedOutputStream;

import java.nio.ByteBuffer;
import java.time.ZonedDateTime;
import java.util.List;
import java.net.UnknownHostException;

/**
 * utility-class, collection of static helper-methods
 *
*/
public final class BC_UTILS
{

/**
 * helper-method to check if a filename does exist
 *
 * @param filename      name of file to be tested
 * @return              does a file of this name exist ?
*/
    public static boolean file_exists( final String filename )
    {
        File f = new File( filename );
        return f.exists();
    }

/**
 * helper-method to create paths of a filename, if they do not exist
 *
 * @param filepath      path of file to be inspected
 * @return              does the file exist, or was it possible to create the path ?
*/
    public static boolean create_paths_if_neccessary( final String filepath )
    {
        File f = new File( filepath );
        boolean res = f.exists();
        if ( !res )
        {
            String parent = f.getParent();
            if ( parent != null )
            {
                File d = new File( parent );
                res = d.exists();
                if ( !res )
                {
                    d.mkdirs();
                    res = d.exists();
                }
            }
            else
                res = true;
        }
        return res;
    }

/**
 * helper-method to save a list of strings to a file
 *
 * @param lines         list of strings to be saved
 * @param filename      path of file to be written into
 * @return              was the operation successful ?
*/
    public static boolean save_to_file( final List< String > lines, final String filename )
    {
        boolean res = create_paths_if_neccessary( filename );
        if ( res )
        {
            BufferedWriter writer = null;
            try
            {
                writer = new BufferedWriter( new FileWriter( new File( filename ) ) );
                if ( writer != null )
                {
                    for ( String line : lines )
                            writer.write( String.format( "%s\n", line ) );
                }
            }
            catch( Exception e ) { e.printStackTrace(); res = false; }
            finally { try { writer.close(); } catch( Exception e ) { e.printStackTrace(); } }
        }
        return res;
    }

/**
 * helper-method to save a ByteBuffer to a file
 *
 * @param buf           ByteBuffer to be written
 * @param filename      path of file to be written into
 * @return              was the operation successful ?
*/
    public static boolean write_to_file( final ByteBuffer buf, final String filename )
    {
        boolean res = create_paths_if_neccessary( filename );
        if ( res )
        {
            BufferedOutputStream os = null;
            try
            {
                os = new BufferedOutputStream( new FileOutputStream( filename ) );
                if ( os != null )
                {
                    os.write( buf.array() );
                    res = true;
                }
            }
            catch( Exception e ) { e.printStackTrace(); res = false; }
            finally { try { os.close(); } catch( Exception e ) { e.printStackTrace(); } }
        }
        return res;
    }

    public static String datetime()
    {
        return String.format( "%s", ZonedDateTime.now() );
    }

/**
 * helper-method the return the dns-name of the local machine
 *
 * @param dflt          the default return value in case of an exception
 * @return              the dns-name of the local machine or the default-value
*/
    public static String get_local_host( final String dflt )
    {
        String res = dflt;
        try
        {
            java.net.InetAddress localMachine = java.net.InetAddress.getLocalHost();
            res = localMachine.getHostName();
        }
        catch ( UnknownHostException e )
        {
            res = dflt;
        }
        return res;
    }

}
