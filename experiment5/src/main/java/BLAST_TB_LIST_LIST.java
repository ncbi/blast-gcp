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
import java.util.Collections;

import static java.lang.Math.min;

class BLAST_TB_LIST_LIST implements Serializable
{
    public ArrayList< BLAST_TB_LIST > list;

    public BLAST_TB_LIST_LIST( final BLAST_TB_LIST elem1 )
    {
        list = new ArrayList<>();
        list.add( elem1 );
    }

    public BLAST_TB_LIST_LIST( final BLAST_TB_LIST_LIST l1, BLAST_TB_LIST_LIST l2 )
    {
        ArrayList< BLAST_TB_LIST > tmp = new ArrayList<>();
        for ( BLAST_TB_LIST e : l1.list )
            tmp.add( e );
        for ( BLAST_TB_LIST e : l2.list )
            tmp.add( e );

        Collections.sort( tmp );
        Integer top_n = tmp.get( 0 ).req.top_n;

        list = new ArrayList<>();
        for ( BLAST_TB_LIST e : tmp.subList( 0, min( tmp.size(), top_n ) ) )
            list.add( e );
    }
}

