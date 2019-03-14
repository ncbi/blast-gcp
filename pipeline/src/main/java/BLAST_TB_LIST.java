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

/**
 * storage for the output of traceback name and layout is coupled with the BLAST_LIB-class and the
 * C++-code
 *
 * @see BLAST_LIB
 */
public final class BLAST_TB_LIST implements Serializable, Comparable<BLAST_TB_LIST> {
  public int oid;
  public int evalue;
  public byte[] asn1_blob;
  public int top_n;

  /**
   * constructor providing values for internal fields
   *
   * @param oid OID - value ( object-ID ? )
   * @param evalue scaled exponent of E-value, to be used for sorting.
   *               The value: -10000 * log(E-value)
   * @param asn1_blob opaque asn1-blob, the traceback-result
   */
  public BLAST_TB_LIST(int oid, int evalue, byte[] asn1_blob) {
    this.oid = oid;
    this.evalue = evalue;
    this.asn1_blob = asn1_blob;
  }

  /**
   * test for empty-ness
   *
   * @return is this traceback-result-list empty ?
   */
  public Boolean isEmpty() {
    return (asn1_blob.length == 0);
  }

  /**
   * overriden comparison, for sorting
   *
   * @param other other instance to compare against
   * @return 0...equal, -1...this > other, +1...this < other
   */
  @Override
  public int compareTo(final BLAST_TB_LIST other) {
    // descending order
    return -Integer.compare( this.evalue, other.evalue );
  }

  private static String toHex(final byte[] blob) {
    String res = "";
    res += "\n        ";
    int brk = 0;
    for (final byte b : blob) {
      res += String.format("%02x", b);
      ++brk;
      if (brk % 4 == 0) {
        res += " ";
      }
      if (brk % 32 == 0) {
        res += "\n        ";
      }
    }
    res += "\n";
    return res;
  }

  /**
   * getter for use by test-code
   *
   * @return returns the object as string
   */
  @Override
  public String toString() {
    String res = "  TBLIST";
    res += String.format("\n  evalue=%d oid=%d", evalue, oid);
    if (asn1_blob != null) {
      res += "\n  " + asn1_blob.length + " bytes in ASN.1 blob\n";
    }
    if (asn1_blob != null) {
      res += toHex(asn1_blob);
    }
    return res;
  }
}
