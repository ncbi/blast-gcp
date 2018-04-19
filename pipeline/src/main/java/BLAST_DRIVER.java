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
import java.util.HashMap;
import java.util.Set;
import java.util.Iterator;
import java.util.Collections;
import java.util.Random;

import static java.lang.Math.min;

import java.nio.ByteBuffer;

import scala.Tuple2;

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
import org.apache.spark.SparkFiles;
import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkEnv;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.util.Collection;

import com.google.pubsub.v1.PubsubMessage;
import com.google.cloud.spark.pubsub.PubsubUtils;

class BLAST_DRIVER extends Thread
{
    private final BLAST_SETTINGS settings;
    private final JavaSparkContext sc;
    private final JavaStreamingContext jssc;

    public BLAST_DRIVER( final BLAST_SETTINGS settings, final List< String > files_to_transfer )
    {
        this.settings = settings;

        SparkConf conf = new SparkConf();
        conf.setAppName( settings.appName );

        conf.set( "spark.streaming.stopGracefullyOnShutdown", "true" );
        conf.set( "spark.streaming.receiver.maxRate", String.format( "%d", settings.receiver_max_rate ) );
        if ( settings.num_executors > 0 )
            conf.set( "spark.executor.instances", String.format( "%d", settings.num_executors ) );
        if ( settings.num_executor_cores > 0 )
            conf.set( "spark.executor.cores", String.format( "%d", settings.num_executor_cores ) );
        if ( !settings.executor_memory.isEmpty() )
            conf.set( "spark.executor.memory", settings.executor_memory );

        sc = new JavaSparkContext( conf );
        sc.setLogLevel( "ERROR" );

        // send the given files to all nodes
        for ( String a_file : files_to_transfer )
            sc.addFile( a_file );

        // create a streaming-context from SparkContext given
        jssc = new JavaStreamingContext( sc, Durations.seconds( settings.batch_duration ) );
    }

    public void stop_blast()
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
            create database-partitions
       =========================================================================================== */
    private JavaRDD< BLAST_PARTITION > make_db_partitions( Broadcast< BLAST_SETTINGS > SETTINGS,
        Broadcast< BLAST_PARTITIONER0 > PARTITIONER0 )
    {
        final List< Tuple2< Integer, BLAST_PARTITION > > db_list = new ArrayList<>();
        for ( int i = 0; i < settings.num_db_partitions; i++ )
        {
            BLAST_PARTITION part = new BLAST_PARTITION( settings.db_location, settings.db_pattern,
                                                        i, settings.flat_db_layout );
            db_list.add( new Tuple2<>( i, part ) );
        }
        return sc.parallelizePairs( db_list, SETTINGS.getValue().num_executors ).partitionBy( PARTITIONER0.getValue() ).map( item -> 
        {
            BLAST_PARTITION part = item._2();

            BLAST_SETTINGS bls = SETTINGS.getValue();
            if ( bls.log_part_prep )
                BLAST_SEND.send( bls, String.format( "preparing %s", part ) );

            return BLAST_LIB_SINGLETON.prepare( part, bls );
        }).cache();
    }

    /* ===========================================================================================
            create source-stream of Strings
       =========================================================================================== */
    private JavaDStream< String > create_socket_stream( Broadcast< BLAST_SETTINGS > SETTINGS )
    {
        JavaDStream< String > res = jssc.socketTextStream( settings.trigger_host, settings.trigger_port ).cache();
        if ( settings.log_request )
        {
            return res.map( item ->
            {
                BLAST_SEND.send( SETTINGS.getValue(), String.format( "REQ via socket: '%s'", item ) );
                return item;
            } ).cache();
        }
        else
        {
            return res.cache();
        }
    }

    private JavaDStream< String > create_pubsub_stream( Broadcast< BLAST_SETTINGS > SETTINGS )
    {
        final DStream< PubsubMessage > pub1 = PubsubUtils.createStream( jssc.ssc(),
            settings.project_id, settings.subscript_id );

        final JavaDStream< PubsubMessage > pub2 = JavaDStream$.MODULE$.fromDStream( pub1,
            scala.reflect.ClassTag$.MODULE$.apply( PubsubMessage.class ) );

        return pub2.map( item ->
        {
            String res = item.getData().toStringUtf8();
            BLAST_SETTINGS bls = SETTINGS.getValue();
            if ( bls.log_request )
                BLAST_SEND.send( bls, String.format( "REQ via pubsub: '%s'", res.substring( 0, 30 ) ) );
            return res;
        }).cache();
    }

    private JavaDStream< String > create_source( Broadcast< BLAST_SETTINGS > SETTINGS )
    {
        BLAST_SETTINGS bls = SETTINGS.getValue();
        Boolean use_socket = ( bls.trigger_port > 0 && !bls.trigger_host.isEmpty() );
        Boolean use_pubsub = ( !bls.project_id.isEmpty() && !bls.subscript_id.isEmpty() );

        if ( use_socket )
        {
            if ( use_pubsub )
            {
                JavaDStream< String > S1 = create_socket_stream( SETTINGS );
                JavaDStream< String > S2 = create_pubsub_stream( SETTINGS );
                return S1.union( S2 ).cache();
            }
            else
                return create_socket_stream( SETTINGS ).cache();
        }
        else
        {
            if ( use_pubsub )
                return create_pubsub_stream( SETTINGS ).cache();
            else
                return null;
        }
    }


    /* ===========================================================================================
            perform prelim search

            IN  :   SRC: JavaPairDStream < BLAST_PARTITION, STRING: REQUEST as JSON >
            OUT :   JavaPairDStream < STRING: 'PARTITION.NR REQ.ID', BLAST_HSP_LIST >
       =========================================================================================== */
    private JavaPairDStream< String, BLAST_HSP_LIST > perform_prelim_search(
            final JavaPairDStream< BLAST_PARTITION, String > SRC,
            Broadcast< BLAST_SETTINGS > SETTINGS )
    {
        return SRC.flatMapToPair( item -> {
            BLAST_SETTINGS bls = SETTINGS.getValue();            

            BLAST_PARTITION part = item._1();
            BLAST_REQUEST req = BLAST_REQUEST_READER.parse( item._2(), bls.top_n ); // REQ-LINE to REQUEST

            // see if we are at a different worker-id now
            if ( bls.log_worker_shift )
            {
                String curr_worker_name = java.net.InetAddress.getLocalHost().getHostName();
                if ( !curr_worker_name.equals( part.worker_name ) )
                {
                    BLAST_SEND.send( bls,
                                     String.format( "pre worker-shift for %d: %s -> %s", part.nr, part.worker_name, curr_worker_name ) );
                }
            }

            // ++++++ this is the where the work happens on the worker-nodes ++++++
            if ( bls.log_job_start )
                BLAST_SEND.send( bls,
                                 String.format( "starting request: '%s' at '%s' ", req.id, part.db_spec ) );

            ArrayList< Tuple2< String, BLAST_HSP_LIST > > ret = new ArrayList<>();
            Integer count = 0;
            try
            {
                BLAST_LIB blaster = BLAST_LIB_SINGLETON.get_lib( part );

                if ( bls.log_sing_request )
                    BLAST_SEND.send( bls, String.format( "prelim SINGLETON.requests( %d ) = %d", part.nr, BLAST_LIB_SINGLETON.get_requests( part ) ) );

                long startTime = System.currentTimeMillis();
                BLAST_HSP_LIST[] search_res = blaster.jni_prelim_search( part, req, "Info" );
                long elapsed = System.currentTimeMillis() - startTime;

                count = search_res.length;
            
                if ( bls.log_job_done )
                    BLAST_SEND.send( bls,
                                     String.format( "request '%s'.'%s' done -> count = %d", req.id, part.db_spec, count ) );

                for ( BLAST_HSP_LIST S : search_res )
                    ret.add( new Tuple2<>( String.format( "%d %s", part.nr, req.id ), S ) );
            }
            catch ( Exception e )
            {
                BLAST_SEND.send( bls,
                                 String.format( "request exeption: '%s on %s' for '%s'", e, req.toString(), part.toString() ) );
            }
            return ret.iterator();
        }).cache();
    }


    /* ===========================================================================================
            calculate cutoff

            IN  :   HSPS: JavaPairDStream< STRING: 'PARTITION.NR REQ.ID', BLAST_HSP_LIST >
            OUT :   JavaPairDStream< STRING: 'REQ.ID', INTEGER : CUTOFF ) >
       =========================================================================================== */

    private JavaPairDStream< String, Integer > calculate_cutoff( final JavaPairDStream< String, BLAST_HSP_LIST > HSPS,
             Broadcast< BLAST_SETTINGS > SETTINGS )
    {
        // key   : req_id
        // value : scores, each having a copy of N for picking the cutoff value
        final JavaPairDStream< String, BLAST_RID_SCORE > TEMP1 = HSPS.mapToPair( item -> {
            BLAST_RID_SCORE ris = new BLAST_RID_SCORE( item._2().max_score, item._2().req.top_n );
            String key = item._1();
            String req_id = key.substring( key.indexOf( ' ' ) + 1, key.length() );
            return new Tuple2< String, BLAST_RID_SCORE >( req_id, ris );
        }).cache();

        // key   : req_id
        // value : list of scores, each having a copy of N for picking the cutoff value
        // this will conentrate all BLAST_RID_SCORE instances for a given req_id on one worker node
        final JavaPairDStream< String, Iterable< BLAST_RID_SCORE > > TEMP2 = TEMP1.groupByKey().cache();

        // key   : req_id
        // value : the cutoff value
        // this will run once for a given req_id on one worker node
        return TEMP2.mapToPair( item -> {
            Integer cutoff = 0;
            Integer top_n = 0;
            ArrayList< Integer > lst = new ArrayList<>();
            for( BLAST_RID_SCORE s : item._2() )
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
            BLAST_SETTINGS bls = SETTINGS.getValue();
            if ( bls.log_cutoff )
                BLAST_SEND.send( bls, String.format( "CUTOFF[ %s ] : %d", item._1(), cutoff ) );
            return new Tuple2<>( item._1(), cutoff );

        }).cache();
    }


    /* ===========================================================================================
            filter by cutoff

            IN  :   HSPS:   JavaPairDStream< STRING: 'PARTITION.NR REQ.ID', BLAST_HSP_LIST >
                    CUTOFF: JavaPairDStream< STRING: 'REQ.ID', INTEGER : CUTOFF ) >
            OUT :   JavaPairDStream< STRING: 'REQ.ID', Tuple2< BLAST_HSP_LIST, INTEGER : CUTOFF > >
       =========================================================================================== */
    private JavaPairDStream< String, Tuple2< BLAST_HSP_LIST, Integer > > filter_by_cutoff(
                                JavaPairDStream< String, BLAST_HSP_LIST > HSPS,
                                JavaPairDStream< String, Integer > CUTOFF,
                                Broadcast< BLAST_PARTITIONER2 > PARTITIONER2 )
    {
		JavaPairDStream< String, String > ACTIVE_PARTITIONS = 
			HSPS.transform( rdd -> rdd.keys().distinct() )
            .mapToPair( item-> new Tuple2<>( item.substring( item.indexOf( ' ' ) + 1, item.length() ), item ) );

        JavaPairDStream< String, Integer > CUTOFF2 = 
			ACTIVE_PARTITIONS.join( CUTOFF ).mapToPair( item -> item._2() ).
                transformToPair( rdd -> rdd.partitionBy( PARTITIONER2.getValue() ) );

        return HSPS.join( CUTOFF2 ).filter( item ->
        {
            BLAST_HSP_LIST hsps = item._2()._1();
            Integer cutoff = item._2()._2();
            return ( cutoff == 0 || hsps.max_score >= cutoff );
        }).cache();

        /*
        return HSPS.join( CUTOFF2, PARTITIONER2.getValue() ) . filter( item ->
        {
            BLAST_HSP_LIST hsps = item._2()._1();
            Integer cutoff = item._2()._2();
            return ( cutoff == 0 || hsps.max_score >= cutoff );
        }).cache();
        */
    }


    /* ===========================================================================================
            perform traceback

            IN  : HSPS: JavaPairDStream< STRING: 'REQ.ID', Tuple2< BLAST_HSP_LIST, INTEGER : CUTOFF > >
            OUT : JavaPairDStream< STRING: 'REQ.ID', BLAST_TB_LIST >
       =========================================================================================== */
    private JavaPairDStream< String, BLAST_TB_LIST > perform_traceback( 
            JavaPairDStream< String, Tuple2< BLAST_HSP_LIST, Integer > > HSPS,
            Broadcast< BLAST_PARTITIONER2 > PARTITIONER2,
            Broadcast< BLAST_SETTINGS > SETTINGS )
    {
        JavaPairDStream< String, BLAST_HSP_LIST > temp1 = HSPS.mapValues( item -> item._1() );

        JavaPairDStream< String, Iterable< BLAST_HSP_LIST > > temp2 = temp1.groupByKey( PARTITIONER2.getValue() );

        return temp2.flatMapToPair( item ->
        {
            String key = item._1();

            BLAST_SETTINGS bls = SETTINGS.getValue();

            //BLAST_SEND.send( bls, String.format( "traceback key = %s", key ) );

            ArrayList< BLAST_HSP_LIST > all_gcps = new ArrayList<>();
            for( BLAST_HSP_LIST e : item._2() )
                all_gcps.add( e );

            BLAST_HSP_LIST [] a = new BLAST_HSP_LIST[ all_gcps.size() ];
            int i = 0;
            for( BLAST_HSP_LIST e : all_gcps )
                a[ i++ ] = e;

            BLAST_PARTITION part = a[ 0 ].part;

            BLAST_LIB blaster = BLAST_LIB_SINGLETON.get_lib( part );
            if ( bls.log_sing_request )
                BLAST_SEND.send( bls, String.format( "traceback SINGLETON.requests( %d ) =%d", part.nr, BLAST_LIB_SINGLETON.get_requests( part ) ) );

            if ( bls.log_worker_shift )
            {
                String curr_worker_name = java.net.InetAddress.getLocalHost().getHostName();
                if ( !curr_worker_name.equals( part.worker_name ) )
                {
                    BLAST_SEND.send( bls,
                                     String.format( "tb worker-shift for %d: %s -> %s", part.nr, part.worker_name, curr_worker_name ) );
                }
            }

            BLAST_TB_LIST [] results = blaster.jni_traceback( a, part, a[ 0 ].req, "Info" );

            ArrayList< Tuple2< String, BLAST_TB_LIST> > ret = new ArrayList<>();            
            for ( BLAST_TB_LIST L : results )
                ret.add( new Tuple2<>( key, L ) );

            return ret.iterator();
        }).cache();
    };


    /* ===========================================================================================
            collect and write traceback
       =========================================================================================== */
    private void write_traceback( JavaPairDStream< String, BLAST_TB_LIST > SEQANNOTS,
                                  Broadcast< BLAST_SETTINGS > SETTINGS ) 
    {
        // key is now req-id
        final JavaPairDStream< String, BLAST_TB_LIST > SEQANNOTS1 = SEQANNOTS.mapToPair( item ->
        {
            String key = item._1();
            return new Tuple2<>( key.substring( key.indexOf( ' ' ) + 1, key.length() ), item._2() );
        } );

        final JavaPairDStream< String, Iterable< BLAST_TB_LIST > > SEQANNOTS2 = SEQANNOTS1.groupByKey();

        final JavaPairDStream< String, ByteBuffer > SEQANNOTS3 = SEQANNOTS2.mapValues( item ->
        {
            List< BLAST_TB_LIST > all_seq_annots = new ArrayList<>();
            for ( BLAST_TB_LIST e : item )
                all_seq_annots.add( e );

            Collections.sort( all_seq_annots );
            Integer top_n = all_seq_annots.get( 0 ).req.top_n;

            List< BLAST_TB_LIST > top_n_seq_annots = all_seq_annots.subList( 0, min( all_seq_annots.size(), top_n ) );
            int sum = 0;
            for ( BLAST_TB_LIST e : top_n_seq_annots )
                sum += e.asn1_blob.length;

			ByteBuffer ret = ByteBuffer.allocate( sum + 8 + 4 );
			byte[] seq_annot_prefix = { (byte) 0x30, (byte) 0x80, (byte) 0xa4, (byte) 0x80, (byte) 0xa1, (byte) 0x80, (byte) 0x31, (byte) 0x80 };
			ret.put( seq_annot_prefix );
			for ( BLAST_TB_LIST e : top_n_seq_annots )
				ret.put( e.asn1_blob );
    		byte[] seq_annot_suffix = { 0, 0, 0, 0 };
			ret.put( seq_annot_suffix );

			return ret;
        });

        SEQANNOTS3.foreachRDD( rdd ->
        {
            long count = rdd.count();
            if ( count > 0 )
            {
                rdd.foreachPartition( iter ->
                {
                    while( iter.hasNext() )
                    {
                        Tuple2< String, ByteBuffer > item = iter.next();
                        String req_id = item._1();
                        ByteBuffer value = item._2();
                        BLAST_SETTINGS bls = SETTINGS.getValue();

                        if ( bls.gs_result_bucket.isEmpty() )
                        {
                            String path = String.format( "%sreq_%s.txt", bls.save_dir, req_id );
                            Integer uploaded = BLAST_HADOOP_UPLOADER.upload( path, value );

                            if ( bls.log_final )
                                BLAST_SEND.send( bls, String.format( "%d bytes written to hadoop '%s'",
                                        uploaded, path ) );
                        }
                        else
                        {
                            String gs_key = String.format( "output/%s.asn1", req_id );
                            Integer uploaded = BLAST_GS_UPLOADER.upload( bls.gs_result_bucket, gs_key, value );

                            if ( bls.log_final )
                                BLAST_SEND.send( bls, String.format( "%d bytes written to gs '%s':'%s'",
                                        uploaded, bls.gs_result_bucket, gs_key ) );
                        }

                    }
                } );

                {
                    BLAST_SETTINGS bls = SETTINGS.getValue();
                    if ( bls.log_final )
                        BLAST_SEND.send( bls,
                                         String.format( "REQUEST DONE: count  = %d ( rdd.id = %d )", count, rdd.id() ) );
                }
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
            Broadcast< BLAST_SETTINGS > SETTINGS = sc.broadcast( settings );
            Broadcast< BLAST_PARTITIONER0 > PARTITIONER0 = sc.broadcast(
                                            new BLAST_PARTITIONER0( settings.num_executors ) );
            Broadcast< BLAST_PARTITIONER1 > PARTITIONER1 = sc.broadcast(
                                            new BLAST_PARTITIONER1( settings.num_executors ) );
            Broadcast< BLAST_PARTITIONER2 > PARTITIONER2 = sc.broadcast(
                                            new BLAST_PARTITIONER2( settings.num_executors ) );

            /* ===========================================================================================
                    create database-sections as a static RDD
               =========================================================================================== */
            final JavaRDD< BLAST_PARTITION > DB_SECS = make_db_partitions( SETTINGS, PARTITIONER0 );
			//final Integer numbases = DB_SECS.map( bp -> bp.getSize() ).reduce( ( x, y ) -> x + y );

            /* ===========================================================================================
                    initialize data-source ( socket as a stand-in for pub-sub )
               =========================================================================================== */
            JavaDStream< String > REQ_STREAM = create_source( SETTINGS );
            if ( REQ_STREAM != null )
            {
                /* ===========================================================================================
                        join request-line from data-source with database-sections
                   =========================================================================================== */
                final JavaPairDStream< BLAST_PARTITION, String > JOB_STREAM
                    = REQ_STREAM.transformToPair( rdd -> 
                                DB_SECS.cartesian( rdd ).partitionBy( PARTITIONER1.getValue() ) ).cache();

                /* ===========================================================================================
                        perform prelim search
                   =========================================================================================== */
                final JavaPairDStream< String, BLAST_HSP_LIST > HSPS = perform_prelim_search( JOB_STREAM, SETTINGS );

                /* ===========================================================================================
                        calculate cutoff ( top-score )
                   =========================================================================================== */
                final JavaPairDStream< String, Integer > CUTOFF  = calculate_cutoff( HSPS, SETTINGS );

                /* ===========================================================================================
                        filter by cutoff
                   =========================================================================================== */
                final JavaPairDStream< String, Tuple2< BLAST_HSP_LIST, Integer> > FILTERED_HSPS 
                    = filter_by_cutoff( HSPS, CUTOFF, PARTITIONER2 );

                /* ===========================================================================================
                        perform traceback
                   =========================================================================================== */
                final JavaPairDStream< String, BLAST_TB_LIST > SEQANNOTS
                    = perform_traceback( FILTERED_HSPS, PARTITIONER2, SETTINGS );

                /* ===========================================================================================
                        collect traceback
                   =========================================================================================== */
                write_traceback( SEQANNOTS, SETTINGS );

                /* ===========================================================================================
                        start the streaming
                   =========================================================================================== */
                jssc.start();
                //System.out.println( "database size: " + numbases.toString() );
                System.out.println( "driver started..." );
                jssc.awaitTermination();
            }
            else
                System.out.println( "invalid source(s)!" );
        }
        catch ( Exception e )
        {
            System.out.println( "Spark exception: " + e );
        }
    }
}
