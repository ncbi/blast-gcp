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

public class BLAST_DB_SETTING implements Serializable
{
    public String key;          /* key from request... ( 'nt', 'nr' ) */
    public String location;     /* where is the location root on the worker? '/tmp/blast/db' */
    public String bucket;       /* where is the source-bucket 'nt_50mb_chunks' */
    public Boolean flat_layout;         /* do we use a subdir for each volume under the location on the worker? */
    public Integer num_locations;       /* how many locations per DATABASE-PART */
    public Integer volume_count;        /* how many volumes does this database have */
    public Long seq_count;              /* how many sequences are in this database?, comes from the manifest */
    public Long seq_lenght;             /* how many elements are in this database?, comes from the manifest */

    public List< CONF_VOLUME > volumes; /* comes either from the manifest, or is created using the ini.json */

    public BLAST_DB_SETTING()
    {
        volumes = new ArrayList<>();
    }

    public Boolean valid()
    {
        if ( key.isEmpty() ) return false;
        if ( location.isEmpty() ) return false;
        if ( bucket.isEmpty() ) return false;
        return true;
    }

    public String missing()
    {
        String S = "";
        if ( key.isEmpty() ) S = S + String.format( "key is missing\n" );
        if ( location.isEmpty() ) S = S + String.format( "(%s).location is missing\n", key );
        if ( bucket.isEmpty() ) S = S + String.format( "(%s).bucket is missing\n", key );
        return S;
    }

    @Override public String toString()
    {
        String S = String.format( "\t(%s).location ........ '%s'\n", key, location );
        S =  S  +  String.format( "\t(%s).bucket .......... '%s'\n", key, bucket );
        S =  S  +  String.format( "\t(%s).flat layout ..... %s\n", key, Boolean.toString( flat_layout ) );
        S =  S  +  String.format( "\t(%s).num_locations ... %d\n", key, num_locations );
        return S;
    }
}

