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

package gov.nih.nlm.ncbi.blast_spark_cluster;

import java.io.Serializable;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;

public class BC_SETTINGS implements Serializable
{
    public String appName = "";
    
    /* REQUESTS */
    public boolean req_use_pubsub = false;
    public String req_pubsub_project_id = "";
    public String req_pubsub_subscript_id = "";
    public boolean req_use_files = false;
    public String req_files_dir = "";
    public boolean req_use_socket = false;
	public Integer req_port_nr = 0;
    public Integer req_max_backlog = 10;

    /* DATABASES */
    HashMap< String, BC_DATABASE_SETTING > dbs; // configured via ini.json section

    /* RESULTS */
	public boolean res_use_gs_bucket = false;
    public String res_gs_bucket = "";
    public String res_gs_pattern = "";
	public boolean res_use_files = false;
    public String res_files_dir = "";
    public String res_files_pattern = "";

    /* CLUSTER */
    public List< String > transfer_files;
    public String  spark_log_level = "ERROR";
    public String  locality_wait = "";
    public boolean set_dyn_alloc = false;
    public boolean with_dyn_alloc = false;
    public String  executor_memory = "";
    public boolean set_shuffle_reduceLocality = false;
    public boolean shuffle_reduceLocality = false;
    public boolean scheduler_fair = false;
	public Integer num_executors = 0;
	public Integer num_executor_cores = 0;
    public Integer parallel_jobs = 1;
    public String  jni_log_level = "INFO";

    /* DEBUG */
    BC_DEBUG_SETTINGS debug;

    public BC_SETTINGS()
    {
        dbs = new HashMap<>();
		transfer_files = new ArrayList<>();
        debug = new BC_DEBUG_SETTINGS();
    }

	/* are the */
    private boolean req_valid()
    {
		boolean res = req_use_pubsub || req_use_files || req_use_socket;
        if ( req_use_pubsub )
			res = !req_pubsub_project_id.isEmpty() &&  !req_pubsub_subscript_id.isEmpty();
		if ( res && req_use_pubsub )
			res = !req_files_dir.isEmpty();
		if ( res && req_use_socket )
			res = req_port_nr != 0;
		return res;
    }

    private boolean dbs_valid()
    {
		if ( dbs.isEmpty() ) return false;
        boolean res = true;
        for ( BC_DATABASE_SETTING e : dbs.values() )
            if ( !e.valid() ) res = false;
        return res;
    }

	private boolean res_valid()
	{
		boolean res = res_use_gs_bucket || res_use_files;
		if ( res_use_gs_bucket )
    		res = !res_gs_bucket.isEmpty() && !res_gs_pattern.isEmpty();
		if ( res && res_use_files )
    		res = !res_files_dir.isEmpty() && !res_files_pattern.isEmpty();
		return res;
	}

    public boolean valid()
    {
        return ( req_valid() && dbs_valid() && res_valid() );
    }

    private String dbs_toString()
    {
        String S = "";
        for ( BC_DATABASE_SETTING e : dbs.values() )
            S = S + e.toString();
        return S;
    }

    @Override public String toString()
    {
        String S = "REQUESTS:\n";
        if ( req_use_pubsub )
            S = S + String.format( "\tpubsub-subscript ... '%s':'%s'\n", req_pubsub_project_id, req_pubsub_subscript_id );
        if ( req_use_files )
            S = S + String.format( "\tfiles-dir .......... '%s'\n", req_files_dir );
        if ( req_use_socket )
            S = S + String.format( "\tport ............... %d\n", req_port_nr );
        S = S + String.format( "\tmax. backlog ....... %d requests\n", req_max_backlog );

        S = S + "\nDATABASES:\n" + dbs_toString();

        S = S + "\nRESULTS:\n";
		if ( res_use_gs_bucket )
            S = S + String.format( "\tbucket ............. %s/%s\n", res_gs_bucket, res_gs_pattern );
		if ( res_use_files )
            S = S + String.format( "\tfiles .............. %s/%s\n", res_files_dir, res_files_pattern );

        S = S + "\nCLUSTER:\n";
        S = S + String.format( "\tappName ............ '%s'\n", appName );
        S = S + String.format( "\ttransfer files ..... %s\n", transfer_files );
        S = S + String.format( "\tspark log level .... '%s'\n", spark_log_level );
		if ( !locality_wait.isEmpty() )
	        S = S + String.format( "\tlocality.wait ...... '%s'\n", locality_wait );
		if ( set_dyn_alloc )
        	S = S + String.format( "\twith_dyn_alloc ..... %s\n", Boolean.toString( with_dyn_alloc ) );
        if ( !executor_memory.isEmpty() )
            S  =  S +  String.format( "\texecutor memory..... %s\n", executor_memory );
		if ( set_shuffle_reduceLocality )
        S = S + String.format( "\treduce_loc ......... %s\n", Boolean.toString( shuffle_reduceLocality ) );

        S = S + String.format( "\tscheduler fair ..... %s\n", Boolean.toString( scheduler_fair ) );
		if ( num_executors > 0 )
	        S = S + String.format( "\tnum-executors ...... %d\n", num_executors );
		if ( num_executor_cores > 0 )
	        S = S + String.format( "\tnum-executor-cores . %d\n", num_executor_cores );
        S = S + String.format( "\tparallel jobs ...... %d\n", parallel_jobs );
        S = S + String.format( "\tjni log level ...... '%s'\n", jni_log_level );

        S = S + "\nDEBUG:\n";
        S = S + debug.toString();
        return S;
    }
}

