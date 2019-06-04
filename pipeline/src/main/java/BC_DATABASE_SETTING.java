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
import java.util.List;
import java.util.ArrayList;

/**
 * common database-settings for one database, 'nt' or 'nr'
 * - stores a key to match the datbase with the request, for instance 'nt'
 * - stores the common location for the databases on the worker, for instance '/tmp/blast/db'
 * - stores the source ( bucket ) for a database, for instance 'gs://nr_50mb_chunks'
 * - stores a flag to indicate, if we want to access the database-chunks directly for on-premise
 * - stores a numer to limit the number of chunks to this value, 0...no limit
 * - stores a list of extensions for each database-chunk
 *
*/
public class BC_DATABASE_SETTING implements Serializable
{
    public String key = "";             /* key for mapping to request... ( 'nt', 'nr' ) */
    public String worker_location = "/tmp/blast/db"; /* where is the location root on the worker? dflt: '/tmp/blast/db' */
    public String source_location = ""; /* bucket or filesystem-path */
    public Boolean direct = false;      /* are we adressing the chunks directly, in case of on-premise */
    public int limit = 0;               /* in case we want to limit the number of db-chunks */
    public List< String > extensions;   /* for nt: nsq, nin, nhr / nr: psq, pin, phr */
    public int ballast = 0;             /* how much (unused) ballast we want to add to each RDD-entry */

/**
 * create instance of BC_DATABASE_SETTING
 * - create empty List
*/
    public BC_DATABASE_SETTING()
    {
        extensions = new ArrayList<>();
    }

/**
 * tests if all neccessary settings are in place
 *
 * @return     are all neccessary settings in place ?
*/
    public Boolean valid()
    {
        return ( !key.isEmpty() && !worker_location.isEmpty() && !source_location.isEmpty() && !extensions.isEmpty() );
    }

/**
 * convert settings to multiline string for debug-purpose
 *
 * @return     multiline string, printing all values of the settings
*/
    @Override public String toString()
    {
        String S = String.format( "\t(%s).worker-loc ...... '%s'\n", key, worker_location );
        S =  S  +  String.format( "\t(%s).source-loc ...... '%s'\n", key, source_location );
        S =  S  +  String.format( "\t(%s).direct .......... %s\n", key, Boolean.toString( direct ) );
        S =  S  +  String.format( "\t(%s).extensions ...... %s\n", key, extensions );
        if ( limit > 0 )
            S =  S  +  String.format( "\t(%s).limit ........... %d\n", key, limit );
        if ( ballast > 0 )
            S =  S  +  String.format( "\t(%s).ballast ......... %d\n", key, ballast );
        return S;
    }
}

