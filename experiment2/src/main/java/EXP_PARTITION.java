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

package gov.nih.nlm.ncbi.exp;

import org.apache.spark.Partition;

public class EXP_PARTITION implements Partition
{
    private static final int MAGIC_NUMBER = 41;
    private static final long serialVersionUID = 4822039463206514988L;

    private final int rddId;
    private final int idx;
    private final String pref_loc;

    public EXP_PARTITION( int rddId, int idx, String pref_loc )
    {
        this.rddId = rddId;
        this.idx = idx;
        this.pref_loc = pref_loc;
    }

    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
 
        EXP_PARTITION that = ( EXP_PARTITION ) o;
        return ( idx == that.idx ) && ( rddId == that.rddId );
    }

    @Override public int hashCode()
    {
        return MAGIC_NUMBER * ( MAGIC_NUMBER + this.rddId ) + this.idx;
    }

    @Override public int index()
    {
        return this.idx;
    }

    public String getPrefLoc()
    {
        return this.pref_loc;
    }

    @Override public String toString()
    {
        return String.format( "%s rddId=%d, idx=%d, pref_loc=%s",
                    getClass().getName(), rddId, idx, pref_loc );
    }
}
