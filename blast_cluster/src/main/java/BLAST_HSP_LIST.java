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
 * storage for the output of blast-search
 * name and layout is coupled with the BLAST_LIB-class and the C++-code
 *
 * @see BLAST_LIB
*/
public final class BLAST_HSP_LIST implements Serializable
{
    public int oid;
    public int max_score;
    public byte[] hsp_blob;


/**
 * constructor providing values for internal fields
 *
 * @param	oid 		OID - value ( object-ID ? )
 * @param	max_score	highest score for search-results
 * @param	hsp_blob	opaque HSP-blob
*/
    public BLAST_HSP_LIST( int oid, int max_score, byte[] hsp_blob )
    {
        this.oid = oid;
        this.max_score = max_score;
        this.hsp_blob = hsp_blob;
    }

/**
 * test for empty-ness
 *
 * @return		is this HSP-list empty ?
*/
    public Boolean isEmpty()
    {
        if (hsp_blob == null) return true;
        return (hsp_blob.length == 0);
    }
}

