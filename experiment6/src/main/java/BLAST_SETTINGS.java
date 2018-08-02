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
import java.util.List;
import java.util.HashMap;

public class BLAST_SETTINGS implements Serializable
{
    public String appName;
    
    /* SOURCE */
    public Boolean use_pubsub_source;
    public String project_id;
    public String subscript_id;
    public Boolean use_hdfs_source;    
    public String hdfs_source_dir;
    public Integer max_backlog;

    /* DB */
    HashMap< String, BLAST_DB_SETTING > dbs; // configured via ini.json section

    /* BLASTJNI */
    public Integer num_db_limit;
    public Integer num_locations;
    public String  jni_log_level;
    public String  manifest_root;
    public String  location;
    public String  lib_name;

    /* RESULTS */
    public String gs_result_bucket;
    public String gs_result_file;
    public String hdfs_result_dir;
    public String hdfs_result_file;
    public String gs_or_hdfs;

    public String gs_status_bucket;
    public String gs_status_file;
    public String gs_status_running;
    public String gs_status_done;
    public String gs_status_error;

    /* SPARK */
    public List< String > transfer_files;
    public String  spark_log_level;
    public String  locality_wait;
    public Integer locality_mode;	/* 0...perfered loc, 1...partitioned, 2...none */
    public Boolean with_dyn_alloc;
    public Integer num_executors;
    public Integer num_executor_cores;
    public String  executor_memory;
    public Boolean shuffle_reduceLocality_enabled;
    public Boolean scheduler_fair;
    public Integer parallel_jobs;
    public Integer comm_port;
	public String  info_tag;
	public Boolean use_jni;

    /* LOG */
    BLAST_LOG_SETTING log;

    public BLAST_SETTINGS()
    {
        log = new BLAST_LOG_SETTING();
        dbs = new HashMap<>();

    }

    public Boolean src_pubsub_valid()
    {
        if ( use_pubsub_source )
            return !project_id.isEmpty() &&  !subscript_id.isEmpty();
        return false;
    }

    public Boolean src_hdfs_valid()
    {
        if ( use_hdfs_source )
            return !hdfs_source_dir.isEmpty();
        return false;
    }

    public Boolean src_valid()
    {
        return src_pubsub_valid() || src_hdfs_valid();
    }

    public Boolean dbs_valid()
    {
        Boolean res = true;
        for ( BLAST_DB_SETTING e : dbs.values() )
            if ( !e.valid() ) res = false;
        return res;
    }

    public Boolean valid()
    {
        if ( !dbs_valid() ) return false;
        if ( !src_valid() ) return false;

        if ( gs_result_bucket.isEmpty() ) return false;
        if ( gs_status_bucket.isEmpty() ) return false;
        return true;
    }

    public String dbs_missing()
    {
        String S = "";
        for ( BLAST_DB_SETTING e : dbs.values() )
            S = S + e.missing();
        return S;
    }

    public String missing()
    {
        String S = dbs_missing();

		if ( use_pubsub_source == null || use_hdfs_source == null )
            S = S + "no source is defined";
		else
		{
		    if ( !use_pubsub_source && !use_hdfs_source )
		        S = S + "no source is defined";
		    else
		    {
		        if ( use_pubsub_source && !src_pubsub_valid() )
		        {
		            if ( project_id.isEmpty() ) S = S + "project_id is missing\n";
		            if ( subscript_id.isEmpty() ) S = S + "subscript_id is missing\n";
		        }
		        if ( use_hdfs_source && !src_hdfs_valid() ) 
		        {
		            if ( hdfs_source_dir.isEmpty() ) S = S + "hdfs-source-dir is missing\n";
		        }
		    }
		}

		if ( gs_result_bucket == null )
			S = S + "gs_result_bucket is missing\n";
		else
		{
        	if ( gs_result_bucket.isEmpty() )
				S = S + "gs_result_bucket is missing\n";
		}

		if ( gs_status_bucket == null )
			S = S + "gs_status_bucket is missing\n";
		else
		{
        	if ( gs_status_bucket.isEmpty() )
				S = S + "gs_status_bucket is missing\n";
		}
        return S;
    }

    private String dbs_toString()
    {
        String S = "";
        for ( BLAST_DB_SETTING e : dbs.values() )
            S = S + e.toString();
        return S;
    }

	private String locality_mode_string()
	{
		if ( locality_mode == 0 )
			return "perfered locality";
		else if ( locality_mode == 1 )
			return "partitioned";
		else
			return "none";
	}

    @Override public String toString()
    {
        String S = "SOURCE:\n";
        if ( use_pubsub_source )
            S = S + String.format( "\tpubsub-subscript ... '%s' : '%s'\n", project_id, subscript_id );
        if ( use_hdfs_source )
            S = S + String.format( "\tHDFS-dir ........... '%s'\n", hdfs_source_dir );
        S = S + String.format( "\tmax. backlog ........... %d requests\n", max_backlog );

        S = S + "\nBLASTJNI:\n";
        S = S + String.format( "\tnum_db_limit ....... %d\n", num_db_limit );
        S = S + String.format( "\tjni-log-level ...... %s\n", jni_log_level );
        S = S + String.format( "\tmanifest root ...... %s\n", manifest_root );
        S = S + String.format( "\tlib-name ........... %s\n", lib_name );

        S = S + "\nDB:\n" + dbs_toString();

        S = S + "\nRESULTS:\n";
        S = S + String.format( "\tGS result .......... '%s' : '%s'\n", gs_result_bucket, gs_result_file );
        S = S + String.format( "\tHDFS result ........ '%s%s'\n", hdfs_result_dir, hdfs_result_file );
        S = S + String.format( "\tGS OR HDFS ......... '%s'\n", gs_or_hdfs );
        S = S + String.format( "\tGS status .......... '%s' : '%s'\n", gs_status_bucket, gs_status_file );
        S = S + String.format( "\tGS status-codes  ... '%s', '%s', '%s'\n", gs_status_running, gs_status_done, gs_status_error );

        S = S + "\nSPARK:\n";
        S = S + String.format( "\tappName ............ '%s'\n", appName );
        S = S + String.format( "\ttransfer files ..... %s\n", transfer_files );
        S = S + String.format( "\tspark log level .... '%s'\n", spark_log_level );
        S = S + String.format( "\tlocality.wait ...... %s\n", locality_wait );
        S = S + String.format( "\tlocality.mode ...... %s\n", locality_mode_string() );
        S = S + String.format( "\twith_dyn_alloc ..... %s\n", Boolean.toString( with_dyn_alloc ) );
        S = S + String.format( "\texecutors .......... %d ( %d cores )\n", num_executors, num_executor_cores );
        S = S + String.format( "\treduce_loc_enabled . %s\n", Boolean.toString( shuffle_reduceLocality_enabled ) );
        S = S + String.format( "\tscheduler fair ..... %s\n", Boolean.toString( scheduler_fair ) );
        S = S + String.format( "\tparallel jobs ...... %d\n", parallel_jobs );
        S = S + String.format( "\tcommunication port . %d\n", comm_port );
        S = S + String.format( "\tinfo-tag ........... '%s'\n", info_tag );
        S = S + String.format( "\tuse jni ............ %s\n", Boolean.toString( use_jni ) );
        if ( !executor_memory.isEmpty() )
            S  =  S +  String.format( "\texecutor memory..... %s\n", executor_memory );

        S = S + log.toString();
        return S;
    }
 
}
