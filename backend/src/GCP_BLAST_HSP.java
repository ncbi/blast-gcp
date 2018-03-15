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

import java.io.*;

class GCP_BLAST_HSP implements Serializable
{
    public final GCP_BLAST_JOB job;
    public final Long qstart, qstop, sstart, sstop;
    public final Integer score;
    
    public GCP_BLAST_HSP( final GCP_BLAST_JOB job,
                final Long qstart, final Long qstop,
                final Long sstart, final Long sstop,
                final Integer score )
    {
        this.job = job;
        this.qstart = qstart;
        this.qstop  = qstop;
        this.sstart = sstart;
        this.sstop  = sstop;
        this.score  = score;
    }
    
    public GCP_BLAST_HSP( final GCP_BLAST_JOB job, final String blast_res )
    {
        this.job = job;
        
        Long t_qstart = 0L;
        Long t_qstop  = 0L;
        Long t_sstart = 0L;
        Long t_sstop  = 0L;
        Integer t_score = 0;
        
        String[] parts = blast_res.replace( "{", "" ).replace( "}", "" ).split( "," );
        for ( String part : parts )
        {
            System.out.println("part is" + part);
            String[] kv = part.trim().split( ":" );
            String key = kv[ 0 ].replaceAll( "\"", "" );
// {"chunk": 4, "RID": "6", "oid": 377616, "score": 43, "qstart": 3, "qstop":
// 70, "sstart": 358, "sstop": 425 }

            System.out.println("key is " + key + " value is " + kv[1]);
            if ( key.equals( "score" ) )
                { t_score = Integer.parseInt( kv[ 1 ].trim() ); }
            else if ( key.equals( "qstart" ) )
                { t_qstart = Long.parseLong( kv[ 1 ].trim() ); }
            else if ( key.equals( "qstop" ) )
                { t_qstop = Long.parseLong( kv[ 1 ].trim() ); }
            else if ( key.equals( "sstart" ) )
                { t_sstart = Long.parseLong( kv[ 1 ].trim() ); }
            else if ( key.equals( "sstop" ) )
                { t_sstop = Long.parseLong( kv[ 1 ].trim() ); }
        }
        this.qstart = t_qstart;
        this.qstop  = t_qstop;
        this.sstart = t_sstart;
        this.sstop  = t_sstop;
        this.score  = t_score;
    }
    
    @Override public String toString()
    {
        return String.format( "%s %d-%d %d-%d (%d)", job.toString(), qstart, qstop, sstart, sstop, score );
    }
}
