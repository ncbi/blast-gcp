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
s*  Government disclaim all warranties, express or implied, including
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
import java.nio.file.Files;
import java.nio.file.Paths;


class BLAST_TB_LIST implements Serializable, Comparable< BLAST_TB_LIST >
{
    public BLAST_PARTITION part;
    public BLAST_REQUEST req;
    public int oid;
    public double evalue;
    public byte[] asn1_blob;

    public BLAST_TB_LIST( int oid, double evalue, byte[] asn1_blob )
    {
        this.oid = oid;
        this.evalue = evalue;
        this.asn1_blob = asn1_blob;
    }

    public Boolean isEmpty()
    {
        return ( asn1_blob.length == 0 );
    }

    public int compareTo( BLAST_TB_LIST other )
    {
        // ascending order
        double v = this.evalue - other.evalue;
        if ( v == 0.0 )
            return 0;
        else if ( v < 0.0 )
            return -1;
        else
            return 1;
    }

    public static String toHex(byte[] blob)
    {
        String res = "";
        res += "\n        ";
        int brk = 0;
        for ( byte b : blob )
        {
            res += String.format( "%02x", b );
            ++brk;
            if ( ( brk % 4 ) == 0 ) res += " ";
            if ( ( brk % 32 ) == 0 ) res += "\n        ";
        }
        res += "\n";
        return res;
    }

    @Override public String toString()
    {
        String res = String.format("  TBLIST( %s %s )", part.toString(), req.toString() );
        res += String.format("\n  evalue=%f oid=%d", evalue, oid);
        if ( asn1_blob != null ) res += "\n  " + asn1_blob.length + " bytes in ASN.1 blob";
        if ( asn1_blob != null ) res += toHex( asn1_blob );
        return res;
    }
}

