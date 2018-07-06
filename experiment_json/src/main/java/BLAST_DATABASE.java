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

import java.util.List;
import java.util.ArrayList;

import scala.Tuple2;
import scala.collection.Seq;
import scala.collection.JavaConversions;
import scala.reflect.ClassTag;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.rdd.RDD;

class BLAST_DATABASE
{
    public JavaRDD< BLAST_DATABASE_PART > DB_SECS;
    public Long total_size;
    public String key;

    BLAST_DATABASE( final BLAST_SETTINGS settings,
                    final Broadcast< BLAST_LOG_SETTING > LOG_SETTING,
                    final JavaSparkContext sc,
                    final BLAST_YARN_NODES nodes,
                    final BLAST_DB_SETTING a_db_setting )
    {
        if ( settings.with_locality )
            DB_SECS = make_db_partitions_with_locality( settings, LOG_SETTING, sc, nodes, a_db_setting );
        else
            DB_SECS = make_db_partitions_no_locality( settings, LOG_SETTING, sc, a_db_setting );

        final JavaRDD< Long > DB_SIZES = DB_SECS.map( item -> BLAST_LIB_SINGLETON.get_size( item ) ).cache();
        total_size = DB_SIZES.reduce( ( x, y ) -> x + y );
        key = a_db_setting.key;

        System.out.println( String.format( "[ %s ] total database size: %,d bytes", key, total_size ) );
    }

    private Integer node_count( final BLAST_SETTINGS settings, final BLAST_YARN_NODES nodes )
    {
        Integer res = nodes.count();
        if ( res == 0 )
            res = settings.num_executors;
        return res;
    }

    /* ===========================================================================================
            create database-partitions
       =========================================================================================== */
    private JavaRDD< BLAST_DATABASE_PART > make_db_partitions_no_locality(
                                final BLAST_SETTINGS settings,
                                final Broadcast< BLAST_LOG_SETTING > LOG_SETTING,
                                final JavaSparkContext sc,
                                final BLAST_DB_SETTING db )
    {
        final List< Tuple2< Integer, BLAST_DATABASE_PART > > db_list = new ArrayList<>();

        Integer i = 0;
        for ( CONF_VOLUME volume : db.volumes )
        {
            BLAST_DATABASE_PART part = new BLAST_DATABASE_PART( i, volume, db.location );
            db_list.add( new Tuple2<>( i++, part ) );
        }

        Integer n = db.volumes.size();
        BLAST_PARTITIONER0 p = new BLAST_PARTITIONER0( n );
        return sc.parallelizePairs( db_list, n ).partitionBy( p ).map( item ->
        {
            BLAST_DATABASE_PART part = item._2();

            BLAST_LOG_SETTING log = LOG_SETTING.getValue();
            if ( log.part_prep )
                BLAST_SEND.send( log, String.format( "preparing %s", part ) );

            return BLAST_LIB_SINGLETON.prepare( part, log );
        } ).cache();
    }

    private JavaRDD< BLAST_DATABASE_PART > make_db_partitions_with_locality(
                                final BLAST_SETTINGS settings,
                                final Broadcast< BLAST_LOG_SETTING > LOG_SETTING,
                                final JavaSparkContext sc,
                                final BLAST_YARN_NODES nodes,
                                final BLAST_DB_SETTING db )
    {
        final List< Tuple2< BLAST_DATABASE_PART, Seq< String > > > part_list = new ArrayList<>();
        BLAST_YARN_NODE_ITER node_iter = new BLAST_YARN_NODE_ITER( nodes );

        Integer i = 0;
        for ( CONF_VOLUME volume : db.volumes )
        {
            BLAST_DATABASE_PART part = new BLAST_DATABASE_PART( i++, volume, db.location );
            List< String > prefered_loc = new ArrayList<>();

            BLAST_YARN_NODE_ITER sub_node_iter = new BLAST_YARN_NODE_ITER( node_iter );
            for ( int j = 0; j < db.num_locations; ++j )
            {
                String host = sub_node_iter.get();
                if ( settings.log.pref_loc )
                    System.out.println( String.format( "adding %s for %s", host, part ) );
                prefered_loc.add( host );
            }
            node_iter.advance();

            Seq< String > prefered_loc_seq = JavaConversions.asScalaBuffer( prefered_loc ).toSeq();
            part_list.add( new Tuple2<>( part, prefered_loc_seq ) );
        }

        // we transform thes list into a Seq
        final Seq< Tuple2< BLAST_DATABASE_PART, Seq< String > > > temp1 = JavaConversions.asScalaBuffer( part_list ).toSeq();

        // we need the class-tag for twice for conversions
        ClassTag< BLAST_DATABASE_PART > tag = scala.reflect.ClassTag$.MODULE$.apply( BLAST_DATABASE_PART.class );

        // this will distribute the partitions to different worker-nodes
        final RDD< BLAST_DATABASE_PART > temp2 = sc.toSparkContext( sc ).makeRDD( temp1, tag );

        // now we transform it into a JavaRDD and perform a mapping-op to eventuall load the
        // database onto the worker-node ( if it is not already there )
        return JavaRDD.fromRDD( temp2, tag ).map( item ->
        {
            BLAST_LOG_SETTING log = LOG_SETTING.getValue();

            if ( log.part_prep )
                BLAST_SEND.send( log, String.format( "preparing %s", item ) );

            return BLAST_LIB_SINGLETON.prepare( item, log );
        } ).cache();
    }

}

