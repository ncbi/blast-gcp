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

package gov.nih.nlm.ncbi.exp;

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

import scala.collection.Seq;
import scala.collection.JavaConversions;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.Partition;

import static scala.collection.JavaConversions.asScalaBuffer;

public abstract class EXP_RDD< T > extends RDD< T >
{
    private static final long serialVersionUID = -7338324965474684518L;

    protected final Broadcast< EXP_RDD_CONFIG > config;

    @SuppressWarnings( "unchecked" )
    public EXP_RDD( SparkContext sc, EXP_RDD_CONFIG aConfig )
    {
        super( sc, scala.collection.Seq$.MODULE$.empty(), ClassTag$.MODULE$.< T >apply( aConfig.getEntityClass() ) );
        config = sc.broadcast( aConfig, ClassTag$.MODULE$.< EXP_RDD_CONFIG >apply( aConfig.getClass() ) );
    }

    @Override public Partition[] getPartitions()
    {
        EXP_RDD_CONFIG cfg = config.value();
        int num_part = cfg.get_num_partitions();

        Partition[] partitions = new EXP_PARTITION[ num_part ];

        for ( int i = 0; i < num_part; ++i )
            partitions[ i ] = new EXP_PARTITION( id(), i, cfg.get_location_by_idx( i ) );

       return partitions;
    }

    @Override public Seq< String > getPreferredLocations( Partition split )
    {
        EXP_PARTITION p = ( EXP_PARTITION ) split;

        List< String > locations = new ArrayList<>();
        locations.add( p.getPrefLoc() );
        return asScalaBuffer( locations );
    }
}

