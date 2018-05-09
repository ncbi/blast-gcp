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
import static java.lang.Math.max;

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
        list = merge_sort( l1.list, l2.list );
    }

    private static ArrayList< BLAST_TB_LIST > merge_sort( List< BLAST_TB_LIST > list1, List< BLAST_TB_LIST > list2 )
    {
        ArrayList< BLAST_TB_LIST > res = new ArrayList<>();

        int top_n = list1.get( 0 ).req.top_n;

        int s1 = list1.size();
        int s2 = list2.size();
        
        int i1 = 0;
        int i2 = 0;
        BLAST_TB_LIST e1 = list1.get( i1 );
        BLAST_TB_LIST e2 = list2.get( i2 );
        while ( e1 != null || e2 != null )
        {
            if ( e1 == null )
            {
                while ( i2 < s2 && res.size() < top_n )
                {
                    res.add( list2.get( i2 ) );
                    i2 += 1;
                }
                e2 = null;
            }
            else if ( e2 == null )
            {
                while ( i1 < s1 && res.size() < top_n )
                {
                    res.add( list1.get( i1 ) );
                    i1 += 1;
                }
                e1 = null;
            }
            else
            {
                if ( res.size() >= top_n )
                {
                    e1 = null;
                    e2 = null;
                }
                else
                {
                    /*
                        0  ... equal
                        -1 ... e1 > e2
                        +1 ... e1 < e2
                    */
                    int cmp = e1.compareTo( e2 );
                    if ( cmp < 0 )
                    {
                        // e1 < e2
                        res.add( e1 );
                        i1 += 1;
                        if ( i1 < s1 )
                            e1 = list1.get( i1 );
                        else
                            e1 = null;
                    }
                    else
                    {
                        // e2 < e1 or e2 == e1
                        res.add( e2 );
                        i2 += 1;
                        if ( i2 < s2 )
                            e2 = list2.get( i2 );
                        else
                            e2 = null;
                    }
                }
            }
        }
        return res;
    }
}

