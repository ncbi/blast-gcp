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
import java.util.HashMap;
import java.util.Set;
import java.util.Iterator;
import java.util.Collection;
import java.util.Collections;
import java.util.Random;

import scala.Option;
import scala.Tuple2;
import scala.collection.Seq;
import scala.collection.JavaConversions;
import scala.reflect.ClassTag;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.dstream.DStream;
import org.apache.spark.streaming.api.java.JavaDStream$;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkFiles;
import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkEnv;
import org.apache.spark.rdd.RDD;

class EXP_DRIVER extends Thread
{
    private final EXP_SETTINGS settings;
    private final EXP_YARN_NODES yarn_nodes;
    private final JavaSparkContext sc;
    private final JavaStreamingContext jssc;

    public EXP_DRIVER( final EXP_SETTINGS settings )
    {
        this.settings = settings;
        this.yarn_nodes = new EXP_YARN_NODES();    // discovers yarn-nodes

        SparkConf conf = new SparkConf();
        conf.setAppName( settings.appName );

        conf.set( "spark.dynamicAllocation.enabled", Boolean.toString( settings.with_dyn_alloc ) );
        conf.set( "spark.streaming.stopGracefullyOnShutdown", "true" );
        if ( settings.num_executors > 0 )
            conf.set( "spark.executor.instances", String.format( "%d", settings.num_executors ) );
        if ( settings.num_executor_cores > 0 )
            conf.set( "spark.executor.cores", String.format( "%d", settings.num_executor_cores ) );
        if ( !settings.executor_memory.isEmpty() )
            conf.set( "spark.executor.memory", settings.executor_memory );
        conf.set( "spark.locality.wait", settings.locality_wait );

        sc = new JavaSparkContext( conf );
        sc.setLogLevel( settings.spark_log_level );

        // create a streaming-context from SparkContext given
        jssc = new JavaStreamingContext( sc, Durations.seconds( settings.batch_duration ) );
    }

    public void stop_driver()
    {
        try
        {
            if ( jssc != null )
                jssc.stop( true, true );
        }
        catch ( Exception e )
        {
            System.out.println( "JavaStreamingContext.stop() : " + e );
        }
    }

    /* ===========================================================================================
            create partition-rdd
       =========================================================================================== */
    private Integer node_count()
    {
        Integer res = yarn_nodes.count();
        if ( res == 0 )
            res = settings.num_executors;
        return res;
    }

    private JavaRDD< EXP_PARTITION > make_partitions( final Broadcast< EXP_SETTINGS > SETTINGS )
    {
        final List< Tuple2< EXP_PARTITION, Seq< String > > > part_list = new ArrayList<>();
        // this happens on the master-node! we need to prepare later...
        for ( int i = 0; i < settings.num_partitions; i++ )
        {
            String host = yarn_nodes.getHost( i % node_count() );

            EXP_PARTITION part = new EXP_PARTITION( i, host );

            if ( settings.log_pref_loc )
                EXP_SEND.send( settings, String.format( "%s", part ) );

            List< String > prefered_loc = new ArrayList<>();            
            prefered_loc.add( host );

            Seq< String > prefered_loc_seq = JavaConversions.asScalaBuffer( prefered_loc ).toSeq();

            part_list.add( new Tuple2<>( part, prefered_loc_seq ) );
        }

        // we transform thes list into a Seq
        final Seq< Tuple2< EXP_PARTITION, Seq< String > > > temp1 = JavaConversions.asScalaBuffer( part_list ).toSeq();

        // we need the class-tag for twice for conversions
        ClassTag< EXP_PARTITION > tag = scala.reflect.ClassTag$.MODULE$.apply( EXP_PARTITION.class );

        // this will distribute the partitions to different worker-nodes
        final RDD< EXP_PARTITION > temp2 = sc.toSparkContext( sc ).makeRDD( temp1, tag );

        // now we transform it into a JavaRDD and perform a mapping-op to eventuall load the
        // database onto the worker-node ( if it is not already there )
        return JavaRDD.fromRDD( temp2, tag ).map( item ->
        {
            EXP_SETTINGS se = SETTINGS.getValue();

            // this happens on the worker-node! we have to prepare here...
            if ( se.log_part_prep )
                EXP_SEND.send( se, String.format( "preparing %s", item ) );

            return item;
        } ).persist( StorageLevel.MEMORY_AND_DISK() );
    }

    /* ===========================================================================================
            create source-stream of Requests
       =========================================================================================== */
    private JavaDStream< EXP_REQUEST > create_socket_stream( final Broadcast< EXP_SETTINGS > SETTINGS )
    {
        JavaDStream< String > tmp = jssc.socketTextStream( settings.trigger_host, settings.trigger_port );
        return tmp.map( item ->
        {
            EXP_SETTINGS se = SETTINGS.getValue();

            EXP_REQUEST res = new EXP_REQUEST( item.trim() );
            if ( se.log_request )
                EXP_SEND.send( se, String.format( "%s", res ) );

            return res;
        } ).cache();
    }

    /* ===========================================================================================
            create stream of Product1 from Partitions and Request
       =========================================================================================== */
    private JavaPairDStream< String, EXP_PRODUCT1 > produce1(
                            final JavaDStream< EXP_REQUEST > REQUESTS,
                            final JavaRDD< EXP_PARTITION > PARTITIONS,
                            final Broadcast< EXP_SETTINGS > SETTINGS )
    {
        final JavaPairDStream< EXP_PARTITION, EXP_REQUEST > JOBS
                = REQUESTS.transformToPair( rdd -> PARTITIONS.cartesian( rdd ) ).cache();
            
        return JOBS.flatMapToPair( item ->
        {
            List< Tuple2< String, EXP_PRODUCT1 > > res = new ArrayList<>();

            EXP_SETTINGS se = SETTINGS.getValue();

            EXP_PARTITION part = item._1();
            EXP_REQUEST req = item._2();
            Random rand = new Random();

            if ( se.log_prod1 )
                EXP_SEND.send( se, String.format( "prod1 for %s", part ) );

            for ( int i = 0; i < 5; ++i )
                res.add( new Tuple2<>( req.id, new EXP_PRODUCT1( req, part.nr, rand.nextInt( 200 ) ) ) );

            return res.iterator();
        }).cache();
    }

    /* ===========================================================================================
            calculate cutoff
       =========================================================================================== */
    private JavaPairDStream< String, Integer > calculate_cutoff(
                            final JavaPairDStream< String, EXP_PRODUCT1 > PROD,
                            final Broadcast< EXP_SETTINGS > SETTINGS )
    {
        // key   : req_id
        // value : scores, each having a copy of N for picking the cutoff value
        final JavaPairDStream< String, EXP_RID_SCORE > TEMP1 = PROD.mapToPair( item -> {
            String key = item._1();
            EXP_PRODUCT1 prod = item._2();
            return new Tuple2< String, EXP_RID_SCORE >( prod.req.id, new EXP_RID_SCORE( prod.score, prod.req.top_n ) );
        }).cache();

        // key   : req_id
        // value : list of scores, each having a copy of N for picking the cutoff value
        // this will conentrate all BLAST_RID_SCORE instances for a given req_id on one worker node
        final JavaPairDStream< String, Iterable< EXP_RID_SCORE > > TEMP2 = TEMP1.groupByKey().cache();

        // key   : req_id
        // value : the cutoff value
        // this will run once for a given req_id on one worker node
        return TEMP2.mapToPair( item -> {
            Integer cutoff = 0;
            Integer top_n = 0;
            ArrayList< Integer > lst = new ArrayList<>();
            for( EXP_RID_SCORE s : item._2() )
            {
                if ( top_n == 0 )
                    top_n = s.top_n;
                lst.add( s.score );
            }
            if ( lst.size() > top_n )
            {
                Collections.sort( lst, Collections.reverseOrder() );
                cutoff = lst.get( top_n - 1 );                    
            }
            EXP_SETTINGS es = SETTINGS.getValue();
            if ( es.log_cutoff )
                EXP_SEND.send( es, String.format( "CUTOFF[ %s ] : %d", item._1(), cutoff ) );
            return new Tuple2<>( item._1(), cutoff );

        }).cache();
    }

    /* ===========================================================================================
            filter by cutoff
       =========================================================================================== */
    private JavaPairDStream< String, EXP_PRODUCT1 > filter_by_cutoff(
                                final JavaPairDStream< String, EXP_PRODUCT1 > PROD,
                                final JavaPairDStream< String, Integer > CUTOFF )
    {
        return PROD.join( CUTOFF ).filter( item ->
        {
            EXP_PRODUCT1 prod = item._2()._1();
            Integer cutoff = item._2()._2();
            return ( cutoff == 0 || prod.score >= cutoff );
        }).mapValues( item -> item._1() ).cache();
    }


    /* ===========================================================================================
            create stream of Product1 from PRODUCT1 and Request
       =========================================================================================== */
    private JavaPairDStream< String, EXP_PRODUCT1 > produce2_v1(
                                final JavaPairDStream< String, EXP_PRODUCT1 > PROD,
                                final JavaRDD< EXP_PARTITION > PARTITIONS,
                                final Broadcast< EXP_SETTINGS > SETTINGS )
    {
        final JavaPairDStream< EXP_PARTITION, Tuple2< String, EXP_PRODUCT1 > > temp1
                = PROD.transformToPair( rdd -> PARTITIONS.cartesian( rdd ) ).cache();

        return temp1.flatMapToPair( item ->
        {
            List< Tuple2< String, EXP_PRODUCT1 > > res = new ArrayList<>();

            EXP_PARTITION a_part = item._1();
            EXP_PRODUCT1  a_prod   = item._2()._2();

            if ( a_part.nr.equals( a_prod.part_nr ) )
            {
                EXP_SETTINGS se = SETTINGS.getValue();

                if ( se.log_prod2 )
                    EXP_SEND.send( se, String.format( "prod2v1: {%s} X {%s}", a_part, a_prod ) );

                String        req_id = item._2()._1();
                res.add( new Tuple2<>( req_id, new EXP_PRODUCT1( a_prod.req, a_part.nr, a_prod.score ) ) );
            }

            return res.iterator();
        }).cache();
    }

    private JavaPairDStream< String, EXP_PRODUCT1 > produce2_v2(
                                final JavaPairDStream< String, EXP_PRODUCT1 > PROD,
                                final JavaPairRDD< Integer, EXP_PARTITION > PARTITIONS,
                                final Broadcast< EXP_SETTINGS > SETTINGS )
    {
        // re-key the product stream by the partition-nr
        final JavaPairDStream< Integer, EXP_PRODUCT1 > temp1
                = PROD.mapToPair( item -> new Tuple2<>( item._2().part_nr, item._2() ) );

        // co-group the product-stream with the keyed partitions
        final JavaPairDStream< Integer, Tuple2< Iterable< EXP_PARTITION >, Iterable< EXP_PRODUCT1 > > > temp2
                = temp1.transformToPair( rdd -> PARTITIONS.cogroup( rdd ) ).cache();

        return temp2.flatMapToPair( item ->
        {
            List< Tuple2< String, EXP_PRODUCT1 > > res = new ArrayList<>();
            EXP_SETTINGS se = SETTINGS.getValue();

            Tuple2< Iterable< EXP_PARTITION >, Iterable< EXP_PRODUCT1 > > part_prod = item._2();

            Iterable< EXP_PARTITION > part_iter = part_prod._1();
            Iterable< EXP_PRODUCT1 > prod_iter = part_prod._2();

            List< EXP_PRODUCT1 > products = new ArrayList<>();
            for ( EXP_PRODUCT1 a_prod : prod_iter )
                products.add( a_prod );

            for ( EXP_PARTITION a_part : part_iter )
            {
                for ( EXP_PRODUCT1 a_prod : products )
                {

                    if ( a_part.nr.equals( a_prod.part_nr ) )
                    {
                        if ( se.log_prod2 )
                            EXP_SEND.send( se, String.format( "prod2v2: {%s} X {%s}", a_part, a_prod ) );

                        res.add( new Tuple2<>( a_prod.req.id, new EXP_PRODUCT1( a_prod.req, a_part.nr, a_prod.score ) ) );
                    }
                }
            }

            return res.iterator();                
        } ).cache();
    }

    private JavaPairDStream< String, EXP_PRODUCT1 > produce2_v3(
                                final JavaPairDStream< String, EXP_PRODUCT1 > PROD,
                                final JavaPairRDD< Integer, EXP_PARTITION > PARTITIONS,
                                final Broadcast< EXP_SETTINGS > SETTINGS )
    {
        // re-key the product stream by the partition-nr
        final JavaPairDStream< Integer, EXP_PRODUCT1 > temp1
                = PROD.mapToPair( item -> new Tuple2<>( item._2().part_nr, item._2() ) );

        // co-group the product-stream with the keyed partitions
        final JavaPairDStream< Integer, Tuple2< EXP_PRODUCT1, EXP_PARTITION > > temp2
                = temp1.transformToPair( rdd -> rdd.join( PARTITIONS ) ).cache();

        return temp2.flatMapToPair( item ->
        {
            List< Tuple2< String, EXP_PRODUCT1 > > res = new ArrayList<>();
            EXP_SETTINGS se = SETTINGS.getValue();

            EXP_PARTITION part = item._2()._2();
            EXP_PRODUCT1 prod = item._2()._1();
            if ( se.log_prod2 )
                EXP_SEND.send( se, String.format( "prod2v3: {%s} X {%s}", part, prod ) );

            res.add( new Tuple2<>( prod.req.id, new EXP_PRODUCT1( prod.req, part.nr, prod.score ) ) );

            return res.iterator();                
        } ).cache();
    }

    /* ===========================================================================================
            consume final product
       =========================================================================================== */
    private void consum_final( final JavaPairDStream< String, EXP_PRODUCT1 > STRM,
                               final Broadcast< EXP_SETTINGS > SETTINGS )
    {
        STRM.foreachRDD( rdd ->
        {
            long count = rdd.count();
            if ( count > 0 )
            {
                rdd.foreachPartition( iter ->
                {
                    EXP_SETTINGS se = SETTINGS.getValue();
                    while( iter.hasNext() )
                    {
                        Tuple2< String, EXP_PRODUCT1 > item = iter.next();
                        String key = item._1();
                        EXP_PRODUCT1 prod = item._2();

                        if ( se.log_final )
                        {
                            EXP_SEND.send( se, String.format( "final: {%s} {%s}", key, prod ) );
                        }
                    }
                } );
            }
        } );
    }

    @Override public void run()
    {
        try
        {
            /* ===========================================================================================
                    broadcast settings
               =========================================================================================== */
            Broadcast< EXP_SETTINGS > SETTINGS = sc.broadcast( settings );
            Broadcast< EXP_YARN_NODES > YARN_NODES = sc.broadcast( yarn_nodes );

            /* ===========================================================================================
                    make PARTITIONS
               =========================================================================================== */
            final JavaRDD< EXP_PARTITION > PARTITIONS = make_partitions( SETTINGS );
            final JavaRDD< Long > P_SIZES = PARTITIONS.map( item -> item.size() );
            final Long total_size = P_SIZES.reduce( ( x, y ) -> x + y );
            
            final JavaPairRDD< Integer, EXP_PARTITION > KEY_PARTITIONS
                        = PARTITIONS.mapToPair( item -> new Tuple2< Integer, EXP_PARTITION >( item.nr, item ) ).cache();

            /* ===========================================================================================
                    make REQUESTS
               =========================================================================================== */
            final JavaDStream< EXP_REQUEST > REQUESTS = create_socket_stream( SETTINGS );

            /* ===========================================================================================
                    produce PRODUCT1 ( PARTITIONS x REQUESTS )
               =========================================================================================== */
            final JavaPairDStream< String, EXP_PRODUCT1 > PROD1
                        = produce1( REQUESTS, PARTITIONS, SETTINGS );

            /* ===========================================================================================
                    produce CUTOFF ( PROD1 ---> < REQ_ID, CUTOFF > )
               =========================================================================================== */
            final JavaPairDStream< String, Integer > CUTOFF
                        = calculate_cutoff( PROD1, SETTINGS );

            /* ===========================================================================================
                    filter by CUTOFF ( PROD1 x < REQ_ID, CUTOFF > ---> PROD1 )
               =========================================================================================== */
            final JavaPairDStream< String, EXP_PRODUCT1 > FILTERED_PROD
                        = filter_by_cutoff( PROD1, CUTOFF );

            /* ===========================================================================================
                    produce PRODUCT2 ( PARTITIONS x PROD2 )
               =========================================================================================== */
            final JavaPairDStream< String, EXP_PRODUCT1 > PROD2
                        //= produce2_v1( FILTERED_PROD, PARTITIONS, SETTINGS );
                        = produce2_v3( FILTERED_PROD, KEY_PARTITIONS, SETTINGS );

            /* ===========================================================================================
                    consume final product
               =========================================================================================== */
            consum_final( PROD2, SETTINGS );

            /* ===========================================================================================
                    start the streaming
               =========================================================================================== */
            jssc.start();
            System.out.println( String.format( "total parts size: %,d bytes", total_size ) );
            System.out.println( "driver started..." );
            jssc.awaitTermination();

        }
        catch ( Exception e )
        {
            System.out.println( "Spark exception: " + e );
        }
    }
}

