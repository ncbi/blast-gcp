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

import java.util.HashMap;

import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.api.java.JavaSparkContext;

class BLAST_DATABASE_MAP
{
    private final HashMap< String, BLAST_DATABASE > databases;

    public BLAST_DATABASE_MAP( final BLAST_SETTINGS settings,
                               final Broadcast< BLAST_SETTINGS > SETTINGS,
                               final JavaSparkContext sc,
                               final BLAST_YARN_NODES nodes )
    {

        databases = new HashMap<>();
        for ( BLAST_DB_SETTING db_setting : settings.db_list.list() )
        {
            BLAST_DATABASE db = new BLAST_DATABASE( settings, SETTINGS, sc, nodes, db_setting );
            databases.put( db.selector, db );
        }
    }

    public BLAST_DATABASE get( final String db_selector )
    {
        BLAST_DATABASE res = databases.get( db_selector );
        return res;
    }
}
