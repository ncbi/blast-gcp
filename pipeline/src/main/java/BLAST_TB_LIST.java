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

import java.io.IOException;
import java.io.Serializable;

/**
 * storage for the output of traceback
 * name and layout is coupled with the BLAST_LIB-class and the C++-code
 *
 * @see BLAST_LIB
*/
public final class BLAST_TB_LIST implements Serializable, Comparable< BLAST_TB_LIST >
{
    static double epsilon = 1.0e-6;
    public int oid;
    public double evalue;
    public byte[] asn1_blob;
    public int top_n;

/**
 * constructor providing values for internal fields
 *
 * @param   oid         OID - value ( object-ID ? )
 * @param   evalue      value to be used for sorting
 * @param   asn1_blob   opaque asn1-blob, the traceback-result
*/
    public BLAST_TB_LIST( int oid, double evalue, byte[] asn1_blob )
    {
        this.oid = oid;
        this.evalue = evalue;
        this.asn1_blob = asn1_blob;
    }

/**
 * test for empty-ness
 *
 * @return      is this traceback-result-list empty ?
*/
    public Boolean isEmpty()
    {
        return ( asn1_blob.length == 0 );
    }

/**
 * fuzzy comparison for sorting
 *
 * @param evalue1   first evalue to compare
 * @param evalue2   second evalue to compare
 * @return          0...evalue1==evalue2, -1...evalue1<evalue2, +1...evalue1>evalue2
*/
    static int FuzzyEvalueComp( double evalue1, double evalue2 )
    {
        /* recommended by BLAST-team */
        if ( evalue1 < ( 1.0 - epsilon ) * evalue2 )
        {
            return -1;
        }
        else if ( evalue1 > ( 1.0 + epsilon ) * evalue2 )
        {
            return 1;
        }
        else
        {
            return 0;
        }
    }

/**
 * overriden comparison, for sorting
 *
 * @param other     other instance to compare against
 * @return          0...equal, -1...this > other, +1...this < other
*/
    @Override public int compareTo( BLAST_TB_LIST other )
    {
        // ascending order
        double delta = ( this.evalue - other.evalue );
        if ( delta < epsilon && delta > -epsilon )
        {
            int oid_delta = ( this.oid - other.oid );
            if ( oid_delta == 0 )
                return 0;
            else if ( oid_delta > 0 )
                return 1;
            else
                return -1;
        }
        else if ( delta > epsilon )
            return 1;
        else
            return -1;
    }

    
}

