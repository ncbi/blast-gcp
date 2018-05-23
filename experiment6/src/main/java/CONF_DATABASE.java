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

public class CONF_DATABASE implements Serializable
{
    public String db_type;
    public String db_manifest_url;
    public String db_tag;
    public Integer volume_count;
    public Long seq_count;
    public Long seq_length;
    public List< CONF_VOLUME > volumes;
    public String updated;
    public String bucket;

    public CONF_DATABASE()
    {
        volumes = new ArrayList<>();
    }

    @Override public String toString()
    {
        String S = String.format( "\ttype ........ '%s'\n", db_type );
        S =  S  +  String.format( "\tmanifest .... '%s'\n", db_manifest_url );
        S =  S  +  String.format( "\tdb_tab ...... '%s'\n", db_tag );
        S =  S  +  String.format( "\tvol_count ... %d\n", volume_count );
        S =  S  +  String.format( "\tseq_count ... %,d\n", seq_count );
        S =  S  +  String.format( "\tseq_length .. %,d\n", seq_length );
        S =  S  +  String.format( "\tupdated ..... '%s'\n", updated );
        S =  S  +  String.format( "\tbucket ...... '%s'\n", bucket );
        for ( CONF_VOLUME v : volumes.subList( 0, 10 ) )
            S = S + v.toString();
        return S;
    }
}

