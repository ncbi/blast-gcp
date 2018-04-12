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

import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import scala.Tuple2;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
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

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Collection;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.InputStreamContent;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.StorageScopes;
import com.google.api.services.storage.model.StorageObject;
import java.io.ByteArrayInputStream;


import com.google.pubsub.v1.PubsubMessage;
import com.google.cloud.spark.pubsub.PubsubUtils;


class BLAST_DRIVER extends Thread
{
    private final BLAST_SETTINGS settings;
    private final JavaSparkContext sc;
    private final JavaStreamingContext jssc;
    private final List< BLAST_PARTITION > db_sec_list;

   /**
    * Create a service for GCS.
    */
    private static Storage buildStorageService() throws GeneralSecurityException, IOException
    {
        HttpTransport transport = GoogleNetHttpTransport.newTrustedTransport();
        JsonFactory jsonFactory = new JacksonFactory();
        GoogleCredential credential = GoogleCredential.getApplicationDefault( transport, jsonFactory );

        if ( credential.createScopedRequired() )
        {
            Collection<String> scopes = StorageScopes.all();
            credential = credential.createScoped( scopes );
        }
        return new Storage.Builder( transport, jsonFactory, credential ).build();
    }

    /**
     * Uploads a file to Google Cloud Storage.
     */
    private static void uploadFile( String bucketName, String key, String content ) throws IOException, GeneralSecurityException
    {
        InputStreamContent contentStream = new InputStreamContent( "text/plain", new ByteArrayInputStream( content.getBytes() ) );
        // Setting the length improves upload performance
        contentStream.setLength( content.getBytes().length );
        // Destination object name
        StorageObject objectMetadata = new StorageObject().setName( key );

        // Do the insert
        Storage.Objects.Insert insertRequest = buildStorageService().objects().insert( bucketName, objectMetadata, contentStream );

        insertRequest.execute();
    }

    public BLAST_DRIVER( final BLAST_SETTINGS settings )
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
        for ( String a_file : settings.files_to_transfer )
            sc.addFile( a_file );

        db_sec_list = create_db_secs();

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

    private List< BLAST_PARTITION > create_db_secs()
    {
        List< BLAST_PARTITION > res = new ArrayList<>();
        for ( int i = 0; i < settings.num_db_partitions; i++ )
            res.add( new BLAST_PARTITION( settings.db_location, settings.db_pattern, i, settings.flat_db_layout ) );
        return res;
    }


    /* ===========================================================================================
            perform prelim search / key = String.format( "%d %s", part.nr, req_req_id )
       =========================================================================================== */
    private JavaPairDStream< String, BLAST_HSP_LIST > perform_prelim_search(
            final JavaPairDStream< BLAST_PARTITION, String > SRC,
            Broadcast< Integer > TOP_N,
            Broadcast< String > LOG_HOST, Broadcast< Integer > LOG_PORT,
            Broadcast< Boolean > LOG_JOB_START, Broadcast< Boolean > LOG_JOB_DONE )
    {
        return SRC.flatMapToPair( item -> {
            ArrayList< Tuple2< String, BLAST_HSP_LIST > > ret = new ArrayList<>();

            BLAST_PARTITION part = item._1();
            BLAST_REQUEST req = new BLAST_REQUEST( item._2(), TOP_N.getValue() ); // REQ-LINE to REQUEST

            // ++++++ this is the where the work happens on the worker-nodes ++++++
            if ( LOG_JOB_START.getValue() )
                BLAST_SEND.send( LOG_HOST, LOG_PORT,
                                 String.format( "starting request: '%s' at '%s' ", req.req_id, part.db_spec ) );

            Integer count = 0;
            try
            {
                String rid = req.req_id;

                long startTime = System.currentTimeMillis();
                BLAST_LIB blaster = new BLAST_LIB();
                BLAST_HSP_LIST[] search_res = blaster.jni_prelim_search( part, req );
                long elapsed = System.currentTimeMillis() - startTime;

                count = search_res.length;
            
                if ( LOG_JOB_DONE.getValue() )
                    BLAST_SEND.send( LOG_HOST, LOG_PORT,
                                     String.format( "request '%s'.'%s' done -> count = %d", rid, part.db_spec, count ) );

                for ( BLAST_HSP_LIST S : search_res )
                    ret.add( new Tuple2<>( String.format( "%d %s", part.nr, req.req_id ), S ) );
            }
            catch ( Exception e )
            {
                BLAST_SEND.send( LOG_HOST, LOG_PORT,
                                     String.format( "request exeption: '%s on %s' for '%s'", e, req.toString(), part.toString() ) );
            }
            return ret.iterator();
        }).cache();
    }


    /* ===========================================================================================
            calculate cutoff
       =========================================================================================== */
    private JavaPairDStream< String, Integer > calculate_cutoff( final JavaPairDStream< String, BLAST_HSP_LIST > HSPS,
             Broadcast< Boolean > LOG_CUTOFF, Broadcast< String > LOG_HOST, Broadcast< Integer > LOG_PORT )
    {
        // key   : req_id
        // value : scores, each having a copy of N for picking the cutoff value
        final JavaPairDStream< String, BLAST_RID_SCORE > TEMP1 = HSPS.mapToPair( item -> {
            BLAST_RID_SCORE ris = new BLAST_RID_SCORE( item._2().max_score, item._2().req.top_n );
            String key = item._1();
            String req_id = key.substring( key.indexOf( ' ' ), key.length() );
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
            if ( LOG_CUTOFF.getValue() )
                BLAST_SEND.send( LOG_HOST, LOG_PORT, String.format( "CUTOFF[ %s ] : %d", item._1(), cutoff ) );
            return new Tuple2<>( item._1(), cutoff );

        }).cache();
    }


    /* ===========================================================================================
            filter by cutoff
       =========================================================================================== */
    private JavaPairDStream< String, Tuple2< BLAST_HSP_LIST, Integer > > filter_by_cutoff(
                                JavaPairDStream< String, BLAST_HSP_LIST > HSPS,
                                JavaPairDStream< String, Integer > CUTOFF,
                                Broadcast< BLAST_PARTITIONER2 > PARTITIONER2,
                                Broadcast< String > LOG_HOST, Broadcast< Integer > LOG_PORT )
    {
		JavaPairDStream< String, String > ACTIVE_PARTITIONS = 
			HSPS.transform( rdd -> rdd.keys().distinct() )
            .mapToPair( item-> new Tuple2<>( item.substring( item.indexOf( ' ' ), item.length() ), item ) );

        JavaPairDStream< String, Integer > CUTOFF2 = 
			ACTIVE_PARTITIONS.join( CUTOFF ).mapToPair( item -> item._2() );

        return HSPS.join( CUTOFF2, PARTITIONER2.getValue() ) . filter( item ->
        {
            BLAST_HSP_LIST hsps = item._2()._1();
            Integer cutoff = item._2()._2();
            return ( cutoff == 0 || hsps.max_score >= cutoff );
        }).cache();
    }


    /* ===========================================================================================
            perform traceback
       =========================================================================================== */
    private JavaPairDStream< String, BLAST_TB_LIST > perform_traceback( 
            JavaPairDStream< String, Tuple2< BLAST_HSP_LIST, Integer > > HSPS,
            Broadcast< String > LOG_HOST, Broadcast< Integer > LOG_PORT )
    {
        JavaPairDStream< String, BLAST_HSP_LIST > temp1 = HSPS.mapValues( item -> item._1() );

        JavaPairDStream< String, Iterable< BLAST_HSP_LIST > > temp2 = temp1.groupByKey();

        return temp2.flatMapToPair( item ->
        {
            String key = item._1();

            // 
            ArrayList< BLAST_HSP_LIST > all_gcps = new ArrayList<>();
            for( BLAST_HSP_LIST e : item._2() )
                all_gcps.add( e );

            BLAST_HSP_LIST [] a = new BLAST_HSP_LIST[ all_gcps.size() ];
            int i = 0;
            for( BLAST_HSP_LIST e : all_gcps )
                a[ i++ ] = e;
            
            BLAST_LIB blaster = new BLAST_LIB();
            BLAST_TB_LIST [] results = blaster.jni_traceback( a, a[ 0 ].part, a[ 0 ].req );

            ArrayList< Tuple2< String, BLAST_TB_LIST> > ret = new ArrayList<>();            
            for ( BLAST_TB_LIST L : results )
                ret.add( new Tuple2<>( key, L ) );

            return ret.iterator();
        }).cache();
    };


    /* ===========================================================================================
            collect and write traceback
       =========================================================================================== */
    private void write_traceback( JavaPairDStream< String, BLAST_TB_LIST > SEQANNOT,
                                Broadcast< String > SAVE_DIR, Broadcast< Boolean > LOG_FINAL,
                                Broadcast< String > LOG_HOST, Broadcast< Integer > LOG_PORT )
    {
        // key is now req-id
        final JavaPairDStream< String, BLAST_TB_LIST > SEQANNOT1 = SEQANNOT.mapToPair( item ->
        {
            String key = item._1();
            return new Tuple2<>( key.substring( key.indexOf( ' ' ) + 1, key.length() ), item._2() );
        } );

        final JavaPairDStream< String, Iterable< BLAST_TB_LIST > > SEQANNOT2 = SEQANNOT1.groupByKey();

        final JavaPairDStream< String, ByteBuffer > SEQANNOT3 = SEQANNOT2.mapValues( item ->
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

        SEQANNOT3.foreachRDD( rdd ->
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

                        if ( LOG_FINAL.getValue() )
                            BLAST_SEND.send( LOG_HOST, LOG_PORT, String.format( "%s : %s", req_id, value.toString() ) );

                        String filename = String.format( "%sreq_%s.txt", SAVE_DIR.getValue(), req_id );
                        //String filename = String.format( "gs:///blastgcp-pipeline-test/output/ReqID_%s.seq-annot.asn1", key );
                        try
                        {
                            Configuration conf = new Configuration();
                            FileSystem fs = FileSystem.get( conf );
                            OutputStream os = fs.create( new Path( filename ) );
                            os.write( value.array() );
                            os.close();

                            int n_bytes = value.array().length;
                            BLAST_SEND.send( LOG_HOST, LOG_PORT, String.format( "%d bytes written: %s", n_bytes, filename ) );

                            try
                            {
                                uploadFile( "blastgcp-pipeline-test", "output/test1", req_id );
                                BLAST_SEND.send( LOG_HOST, LOG_PORT, "upload done" );
                            }
                            catch ( Exception e_up )
                            {
                                BLAST_SEND.send( LOG_HOST, LOG_PORT, String.format( "upload : %s", e_up.toString() ) );
                            }

                        }
                        catch ( Exception e )
                        {
                            BLAST_SEND.send( LOG_HOST, LOG_PORT, String.format( "fs-ex : %s", e.toString() ) );
                        }
                    }
                } );

                if ( LOG_FINAL.getValue() )
                    BLAST_SEND.send( LOG_HOST, LOG_PORT,
                                 String.format( "REQUEST DONE: count  = %d ( rdd.id = %d )", count, rdd.id() ) );

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
            Broadcast< String > LOG_HOST = sc.broadcast( settings.log_host );
            Broadcast< Integer > LOG_PORT = sc.broadcast( settings.log_port );
            Broadcast< String > SAVE_DIR = sc.broadcast( settings.save_dir );
            Broadcast< Boolean > LOG_REQUEST = sc.broadcast( settings.log_request );
            Broadcast< Boolean > LOG_JOB_START = sc.broadcast( settings.log_job_start );
            Broadcast< Boolean > LOG_JOB_DONE = sc.broadcast( settings.log_job_done );
            Broadcast< Boolean > LOG_CUTOFF = sc.broadcast( settings.log_cutoff );
            Broadcast< Boolean > LOG_FINAL = sc.broadcast( settings.log_final );
            Broadcast< Integer > TOP_N = sc.broadcast( settings.top_n );
            Broadcast< BLAST_PARTITIONER1 > PARTITIONER1 = sc.broadcast(
                                            new BLAST_PARTITIONER1( settings.num_workers ) );
            Broadcast< BLAST_PARTITIONER2 > PARTITIONER2 = sc.broadcast(
                                            new BLAST_PARTITIONER2( settings.num_workers ) );

            /* ===========================================================================================
                    create database-sections as a static RDD
               =========================================================================================== */
            final JavaRDD< BLAST_PARTITION > DB_SECS = sc.parallelize( db_sec_list ).cache();
			final Integer numbases = DB_SECS.map( bp -> bp.getSize() ).reduce( ( x, y ) -> x + y );

            /* ===========================================================================================
                    initialize data-source ( socket as a stand-in for pub-sub )
               =========================================================================================== */
            /*
            final DStream< PubsubMessage > pub1 = PubsubUtils.createStream( jssc.ssc(),
                settings.project_id, settings.subscript_id );

            final JavaDStream< PubsubMessage > pub2 = JavaDStream$.MODULE$.fromDStream( pub1,
                scala.reflect.ClassTag$.MODULE$.apply( PubsubMessage.class ) );

            final JavaDStream< String > REQ_STREAM = pub2.map( item -> item.getData().toStringUtf8() );
            */

            final JavaDStream< String > REQ_STREAM
                = jssc.socketTextStream( settings.trigger_host, settings.trigger_port ).cache();

            /* ===========================================================================================
                    join request-line from data-source with database-sections
               =========================================================================================== */
            final JavaPairDStream< BLAST_PARTITION, String > JOINED_REQ_STREAM
                = REQ_STREAM.transformToPair( rdd -> 
                            DB_SECS.cartesian( rdd ).partitionBy( PARTITIONER1.getValue() ) ).cache();

            /* ===========================================================================================
                    perform prelim search
               =========================================================================================== */
            final JavaPairDStream< String, BLAST_HSP_LIST > HSPS
                = perform_prelim_search( JOINED_REQ_STREAM, TOP_N, LOG_HOST, LOG_PORT,
                                         LOG_JOB_START, LOG_JOB_DONE );

            /* ===========================================================================================
                    calculate cutoff ( top-score )
               =========================================================================================== */
            final JavaPairDStream< String, Integer > CUTOFF 
                = calculate_cutoff( HSPS, LOG_CUTOFF, LOG_HOST, LOG_PORT );

            /* ===========================================================================================
                    filter by cutoff
               =========================================================================================== */
            final JavaPairDStream< String, Tuple2< BLAST_HSP_LIST, Integer> > FILTERED 
                = filter_by_cutoff( HSPS, CUTOFF, PARTITIONER2, LOG_HOST, LOG_PORT );

            /* ===========================================================================================
                    perform traceback
               =========================================================================================== */
            final JavaPairDStream< String, BLAST_TB_LIST > SEQANNOT
                = perform_traceback( FILTERED, LOG_HOST, LOG_PORT );

            /* ===========================================================================================
                    collect traceback
               =========================================================================================== */
            write_traceback( SEQANNOT, SAVE_DIR, LOG_FINAL, LOG_HOST, LOG_PORT );

            /* ===========================================================================================
                    start the streaming
               =========================================================================================== */
            jssc.start();
            System.out.println( "database size: " + numbases.toString() );
            System.out.println( "driver started..." );
            jssc.awaitTermination();
        }
        catch ( Exception e )
        {
            System.out.println( "Spark exception: " + e );
        }
    }
}
