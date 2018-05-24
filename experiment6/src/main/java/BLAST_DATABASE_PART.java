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
import java.net.InetAddress;

class BLAST_DATABASE_PART implements Serializable
{
    public final Integer nr;
    public final String db_spec;
    public String worker_name;
    public final CONF_VOLUME volume;

    public BLAST_DATABASE_PART( final BLAST_DATABASE_PART other )
    {
        nr         = other.nr;
        db_spec    = other.db_spec;
        volume     = other.volume;
        worker_name= other.worker_name;
    }

    public BLAST_DATABASE_PART( final Integer a_nr, final CONF_VOLUME a_volume, final String location )
    {
        nr      = a_nr;
        db_spec = String.format( "%s/%s/%s", location, a_volume.name, a_volume.name );
        volume  = a_volume;
    }

    public BLAST_DATABASE_PART enter_worker_name()
    {
        try
        {
            this.worker_name = java.net.InetAddress.getLocalHost().getHostName();
        }
        catch ( Exception e )
        {
            this.worker_name = "unknown";
        }

        return new BLAST_DATABASE_PART( this );
    }

    @Override public String toString()
    {
        return String.format( "part( %s.%d: '%s' )", volume.key, nr, volume.name );
    }

    public Integer getPartition( Integer num_partitions )
    {
        if ( num_partitions > 0 )
            return ( nr % num_partitions );
        else
            return 0;
    }
}

