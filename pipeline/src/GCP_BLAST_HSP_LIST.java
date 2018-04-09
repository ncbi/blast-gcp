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

/*
* FIX or CONVINCE ME
*  The GCP_BLAST_HSP_LIST should not try to represent the entire result of
*  a preliminary search. Rather, it is the result of a SINGLE PAIRING between
*  the query sequence and a single sequence within the db.
*
*  While I get that it's feasible that this single object could represent
*  an entire prelim search, because we won't actually care about the oid
*  during reduction, the oid needs to be represented SOMEWHERE and it should
*  not be included in the HSP blob, due to redundancy (and this is what
*  you're actually doing).
*
*  How is reduction supposed to occur with this structure? We're going
*  to merge all of the scores and hsp blobs into pairs, with all of the
*  same from other nodes, top N them, and then... ?
*
*  Here's more along the lines of what I expected:

   class GCP_BLAST_HSP_LIST implements Serializable
   {
       public int max_score;
       public byte [] hsp_blob;
       public long elapsed;

       GCP_BLAST_HSP_LIST ( int max_score, long blob_size, long elapsed )
       {
           ...
           hsp_blob = new byte [ blob_size ];
           ...
       }
   }
*
*  I'd expect that it would be created from within the loop that is
*  walking the blast hsp_list in C++, after deciding upon a cutoff,
*  whereupon you'd pass in the size needed for the hsp blob, let
*  Java allocate it, and then use its internal memory for storage
*  post factum.
*/
class GCP_BLAST_HSP_LIST implements Serializable {
  public GCP_BLAST_PARTITION partitionobj;
  public GCP_BLAST_REQUEST requestobj;
  public int oid;
  public int max_score;
  public byte[] hsp_blob;

  // CMT - would expect that the constructor would take oid, max-score and size of hsp_blob as
  // parameters
  // the idea behind size of hsp_blob was that you would use that memory to flatten the HSP set, but
  // if that's
  // not practical, then...
  public GCP_BLAST_HSP_LIST() {
    this.oid = -1;
    this.max_score = -1;
    hsp_blob = new byte[0];
  }

  public Boolean isEmpty() {
    if (hsp_blob == null) return true;

    return (hsp_blob.length == 0);
  }

  @Override
  public String toString() {
    String res = "";
    res +=
        String.format(
            "  HSPLIST( partition=%s request=%s )", partitionobj.toString(), requestobj.toString());
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
