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

public class CONF_VOLUME implements Serializable
{
    public String name;
    public List< CONF_VOLUME_FILE > files;

    public CONF_VOLUME( final String a_name )
    {
        name = a_name;
        files = new ArrayList<>();
    }

    public Boolean valid()
    {
        if ( name.isEmpty() ) return false;
        if ( files.isEmpty() ) return false;
        for ( CONF_VOLUME_FILE f : files )
        {
            if ( !f.valid() ) 
                return false;
        }
        return true;
    }

    public String missing()
    {
        String S = "";
        if ( name.isEmpty() ) S = S + "manifest.json : volume.name missing\n";
        if ( files.isEmpty() ) S = S + "manifest.json : volume.files missing\n";
        for ( CONF_VOLUME_FILE f : files )
            S = S + f.missing();
        return S;
    }

    @Override public String toString()
    {
        String S = String.format( "\t\tvolume .... '%s'\n", name );
        for ( CONF_VOLUME_FILE f : files )
            S = S + f.toString();
        return S;
    }

}

