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
    public String selector;
    public String location;
    public String pattern;
    public String bucket;
    public Boolean flat_layout;
    public Integer num_partitions;
    public List< String > extensions;

    public BLAST_DB_SETTING()
    {
        extensions = new ArrayList<>();
    }

    public Boolean valid()
    {
        if ( bucket.isEmpty() ) return false;
        if ( num_partitions < 1 ) return false;
        if ( extensions.isEmpty() ) return false;
        return true;
    }

    public String missing()
    {
        String S = "";
        if ( bucket.isEmpty() ) S = S + String.format( "(%s).bucket is missing\n", selector );
        if ( num_partitions < 1 ) S = S + String.format( "(%s).num_partitions < 1\n", selector );
        if ( extensions.isEmpty() ) S = S + String.format( "(%s).extensions are missing\n", selector );
        return S;
    }

    @Override public String toString()
    {
        String S = String.format( "\t(%s).location ........ '%s'\n", selector, location );
        S =  S  +  String.format( "\t(%s).pattern ......... '%s'\n", selector, pattern );
        S =  S  +  String.format( "\t(%s).bucket .......... '%s'\n", selector, bucket );
        S =  S  +  String.format( "\t(%s).flat layout ..... %s\n", selector, Boolean.toString( flat_layout ) );
        S =  S  +  String.format( "\t(%s).num_partitions .. %d\n", selector, num_partitions );
        S =  S  +  String.format( "\t(%s).extensions ...... %s\n", selector, extensions );
        return S;
    }
}

