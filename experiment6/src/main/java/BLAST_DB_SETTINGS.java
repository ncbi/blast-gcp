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
import java.util.Collection;
import java.util.HashMap;

public class BLAST_DB_SETTINGS implements Serializable
{
    private HashMap< String, BLAST_DB_SETTING > dbs;

    public BLAST_DB_SETTINGS()
    {
        dbs = new HashMap<>();
    }

    public void put( final BLAST_DB_SETTING obj )
    {
        dbs.put( obj.selector, obj );
    }

    public Collection< BLAST_DB_SETTING > list()
    {
        return dbs.values();
    }

    public BLAST_DB_SETTING get( final String selector )
    {
        return dbs.get( selector );
    }

    public Boolean valid()
    {
        Boolean res = true;
        for ( BLAST_DB_SETTING e : dbs.values() )
            if ( !e.valid() ) res = false;
        return res;
    }

    public String missing()
    {
        String S = "";
        for ( BLAST_DB_SETTING e : dbs.values() )
            S = S + e.missing();
        return S;
    }

    @Override public String toString()
    {
        String S = "";
        for ( BLAST_DB_SETTING e : dbs.values() )
            S = S + e.toString();
        return S;
    }
}

