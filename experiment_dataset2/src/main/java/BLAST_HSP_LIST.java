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

final class BLAST_HSP_LIST implements Serializable
{
    public BLAST_PARTITION part;
    public BLAST_REQUEST req;
    public int oid;
    public int max_score;
    public byte[] hsp_blob;

    public BLAST_HSP_LIST( int oid, int max_score, byte[] hsp_blob )
    {
        this.oid = oid;
        this.max_score = max_score;
        this.hsp_blob = hsp_blob;
    }

    public Boolean isEmpty()
    {
        if (hsp_blob == null) return true;
        return (hsp_blob.length == 0);
    }

    @Override public String toString()
    {
        String res = "";
        res += String.format("  HSPLIST( partition=%s request=%s )", part.toString(), req.toString());
        res += "\n         oid = " + oid;
        res += "\n   max_score = " + max_score;
        res += "\n" + String.format("        blob = %d bytes:", hsp_blob.length);
        res += "\n      ";
        int brk = 0;
        for (int i = 0; i != hsp_blob.length; ++i) {
          byte b = hsp_blob[i];
          res += String.format("%02x", b);
          ++brk;
          if ((brk % 4) == 0) res += " ";
        }
        res += "\n";
        return res;
    }
}

