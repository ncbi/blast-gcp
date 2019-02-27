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

class BC_REQUEST implements Serializable
{
    public String id, db, query_seq, program, params, ack_id;
    public Integer top_n_prelim;
    public Integer top_n_traceback;

	public Boolean valid()
	{
    	if ( id == null ) return false;
		if ( id.isEmpty() ) return false;
		if ( db == null ) return false;
		if ( db.isEmpty() ) return false;
		if ( program == null ) return false;
		if ( program.isEmpty() ) return false;
		if ( top_n_prelim == null ) return false;
		if ( top_n_prelim == 0 ) return false;
		if ( top_n_traceback == null ) return false;
		if ( top_n_traceback == 0 ) return false;
		if ( query_seq == null ) return false;
		if ( query_seq.isEmpty() ) return false;
		return true;
	}

    // @TODO: Need HashCode
    @Override public boolean equals( Object other )
    {
        if ( other == null ) return false;
        if ( !BC_REQUEST.class.isAssignableFrom( other.getClass() ) ) return false;
        final BC_REQUEST other_entry = ( BC_REQUEST ) other;
        return id.equals( other_entry.id );
    }

    @Override public String toString()
    {
        return String.format( "req( rid:'%s' db_tag:'%s' prog:'%s' top_n:%d,%d, params:'%s' )",
                id, db, program, top_n_prelim, top_n_traceback, params );
    }
}

