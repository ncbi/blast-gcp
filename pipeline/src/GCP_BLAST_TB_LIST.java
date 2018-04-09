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
import java.util.Comparator;
import java.nio.file.Files;
import java.nio.file.Paths;

/*
 * FIX
 *  You're still going to be passing back a list of tuples
 *     ( job, oid, min-evalue, hsps [] )
 *  where the hsps will be in ASN.1, essentially.
 */

class GCP_BLAST_TB_LIST implements Serializable, Comparable< GCP_BLAST_TB_LIST >
{
    public GCP_BLAST_PARTITION partitionobj;
    public GCP_BLAST_REQUEST requestobj;
    public int oid;
    public double evalue;
    public byte[] asn1_blob;

    public GCP_BLAST_TB_LIST( int oid, double evalue, byte[] asn1_blob )
    {
        this.oid = oid;
        this.evalue = evalue;
        this.asn1_blob = asn1_blob;
    }

    public Boolean isEmpty()
    {
        return (asn1_blob.length == 0);
    }

  // CMT - I'm not sure you're supposed to combine the ASN.1 here, or after
  // reduction by evalue, or both.
  // ANS - Neither am I, keeping as convenience function for now.
  private static byte[] combine(byte[][] asn1_blobs) {
    int sz = 0;

    for (byte[] b : asn1_blobs) {
      sz += b.length;
    }

    byte[] seq_annot_prefix = {
      (byte) 0x30,
      (byte) 0x80,
      (byte) 0xa4,
      (byte) 0x80,
      (byte) 0xa1,
      (byte) 0x80,
      (byte) 0x31,
      (byte) 0x80
    };
    sz += seq_annot_prefix.length;
    byte[] seq_annot_suffix = {0, 0, 0, 0};
    sz += 4 * asn1_blobs.length; // Append 0x00 (4 of them) for each seq-align

    int pos = 0;
    byte[] bytes = new byte[sz];
    System.arraycopy(seq_annot_prefix, 0, bytes, pos, seq_annot_prefix.length);
    pos += seq_annot_prefix.length;
    for (byte[] b : asn1_blobs) {
      System.arraycopy(b, 0, bytes, pos, b.length);
      pos += b.length;
    }
    for (byte[] b : asn1_blobs) {
      System.arraycopy(seq_annot_suffix, 0, bytes, pos, seq_annot_suffix.length);
      pos += seq_annot_suffix.length;
    }
    return bytes;
  }

  // FIX: Take a File object rather than filename
  public static boolean save(String filename, byte[][] asn1_blobs) {
    byte[] bytes = combine(asn1_blobs);

    try {
      Files.write(Paths.get(filename), bytes);
    } catch (IOException e) {
      return false;
    }
    return true;
  }


  public static String toHex(byte[] blob) {
    String res = "";
    res += "\n        ";
    int brk = 0;
    for (byte b : blob) {
      res += String.format("%02x", b);
      ++brk;
      if ((brk % 4) == 0) res += " ";
      if ((brk % 32) == 0) res += "\n        ";
    }
    res += "\n";
    return res;
  }

    public int compareTo( GCP_BLAST_TB_LIST other )
    {
		//ascending order
        double v = this.evalue - other.evalue;
        if ( v == 0.0 )
            return 0;
        else if ( v < 0.0 )
            return -1;
        else
            return 1;
	}

  @Override
  public String toString() {
    String res = String.format("  TBLIST( %s %s )", partitionobj.toString(), requestobj.toString());
    res += String.format("\n  evalue=%f oid=%d", evalue, oid);
    if (asn1_blob != null) res += "\n  " + asn1_blob.length + " bytes in ASN.1 blob";

    if (asn1_blob != null) {
      res += toHex(asn1_blob);
    }
    return res;
  }
}
