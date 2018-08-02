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

class REQUESTQ_ENTRY
{
    public final BLAST_REQUEST request;
    public String ack_id;

    public REQUESTQ_ENTRY( final BLAST_REQUEST a_request )
    {
        this.request = a_request;
        this.ack_id = null;
    }

    public REQUESTQ_ENTRY( final BLAST_REQUEST a_request, final String a_ack_id )
    {
        this.request = a_request;
        this.ack_id = a_ack_id;
    }

    @Override public boolean equals( Object other )
    {
        if ( other == null ) return false;
        if ( !REQUESTQ_ENTRY.class.isAssignableFrom( other.getClass() ) ) return false;
        final REQUESTQ_ENTRY other_entry = ( REQUESTQ_ENTRY ) other;
        if ( other_entry.request == null ) return false;
        return this.request.id.equals( other_entry.request.id );
    }
}

