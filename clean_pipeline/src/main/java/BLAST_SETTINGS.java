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

public class BLAST_SETTINGS implements Serializable
{
    public String appName;
    
    /* SOURCE */
    public Boolean use_pubsub_source;
    public String project_id;
    public String subscript_id;
    public Boolean use_socket_source;
    public String trigger_host;
    public Integer trigger_port;
    public Boolean use_hdfs_source;    
    public String hdfs_source_dir;
    public Integer receiver_max_rate;

    /* DB */
    public String db_location;
    public String db_pattern;
    public String db_bucket;
    public Boolean flat_db_layout;
    public Integer num_db_partitions;

    /* BLASTJNI */
    public Integer top_n;
    public String  jni_log_level;

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
    public String  spark_log_level;
    public Integer batch_duration;
    public String  locality_wait;
    public Boolean with_locality;
    public Boolean with_dyn_alloc;
    public Integer num_executors;
    public Integer num_executor_cores;
    public String  executor_memory;

    /* LOG */
    public String log_host;
    public Integer log_port;
    public Boolean log_request;
    public Boolean log_job_start;
    public Boolean log_job_done;
    public Boolean log_cutoff;
    public Boolean log_final;
    public Boolean log_part_prep;
    public Boolean log_worker_shift;
    public Boolean log_pref_loc;
    public Boolean log_db_copy;

    public Boolean src_pubsub_valid()
    {
        if ( use_pubsub_source )
            return !project_id.isEmpty() &&  !subscript_id.isEmpty();
        return false;
    }

    public Boolean src_socket_valid()
    {
        if ( use_socket_source )
            return !trigger_host.isEmpty() && trigger_port > 0;
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
        return src_pubsub_valid() || src_socket_valid() || src_hdfs_valid();
    }

    public Boolean valid()
    {
        if ( db_bucket.isEmpty() ) return false;
        if ( num_db_partitions == 0 ) return false;
        if ( top_n == 0 ) return false;

        if ( !src_valid() ) return false;

        if ( gs_result_bucket.isEmpty() ) return false;
        if ( gs_status_bucket.isEmpty() ) return false;
        return true;
    }

    public String missing()
    {
        String S = "";
        if ( db_bucket.isEmpty() ) S = S + "db_bucket is missing\n";
        if ( num_db_partitions == 0 ) S = S + "num_db_partitions is 0\n";
        if ( top_n == 0 ) S = S + "top_n is 0\n";

        if ( !use_pubsub_source && !use_socket_source && !use_hdfs_source )
            S = S + "no source is defined";
        else
        {
            if ( use_pubsub_source && !src_pubsub_valid() )
            {
                if ( project_id.isEmpty() ) S = S + "project_id is missing\n";
                if ( subscript_id.isEmpty() ) S = S + "subscript_id is missing\n";
            }
            if ( use_socket_source && !src_socket_valid() ) 
            {
                if ( trigger_host.isEmpty() ) S = S + "trigger-host is missing\n";
                if ( trigger_port < 1 ) S = S + "trigger-port is invalid\n";
            }
            if ( use_hdfs_source && !src_hdfs_valid() ) 
            {
                if ( hdfs_source_dir.isEmpty() ) S = S + "hdfs-source-dir is missing\n";
            }
        }

        if ( gs_result_bucket.isEmpty() ) S = S + "gs_result_bucket is missing\n";
        if ( gs_status_bucket.isEmpty() ) S = S + "gs_status_bucket is missing\n";
        return S;
    }

    @Override public String toString()
    {
        String S = "SOURCE:\n";
        if ( use_pubsub_source )
            S = S + String.format( "\tpubsub-subscript ... '%s' : '%s'\n", project_id, subscript_id );
        if ( use_socket_source )
            S = S + String.format( "\ttrigger_host ....... %s:%d\n", trigger_host, trigger_port );
        if ( use_hdfs_source )
            S = S + String.format( "\tHDFS-dir ........... '%s'\n", hdfs_source_dir );
        S = S + String.format( "\trec. max. rate ..... %d per second\n", receiver_max_rate );

        S = S + "\nDB:\n";
        S = S + String.format( "\tdb_location ........ '%s'\n", db_location );
        S = S + String.format( "\tdb_pattern ......... '%s'\n", db_pattern );
        S = S + String.format( "\tdb_bucket .......... '%s'\n", db_bucket );
        S = S + String.format( "\tflat db layout...... %s\n", Boolean.toString( flat_db_layout ) );
        S = S + String.format( "\tnum_db_partitions .. %d\n", num_db_partitions );

        S = S + "\nBLASTJNI:\n";
        S = S + String.format( "\tdflt top_n ......... %d\n", top_n );
        S = S + String.format( "\tjni-log-level ...... %s\n", jni_log_level );

        S = S + "\nRESULTS:\n";
        S = S + String.format( "\tGS result .......... '%s' : '%s'\n", gs_result_bucket, gs_result_file );
        S = S + String.format( "\tHDFS result ........ '%s%s'\n", hdfs_result_dir, hdfs_result_file );
        S = S + String.format( "\tGS OR HDFS ......... '%s'\n", gs_or_hdfs );
        S = S + String.format( "\tGS status .......... '%s' : '%s'\n", gs_status_bucket, gs_status_file );
        S = S + String.format( "\tGS status-codes  ... '%s', '%s', '%s'\n", gs_status_running, gs_status_done, gs_status_error );

        S = S + "\nSPARK:\n";
        S = S + String.format( "\tappName ............ '%s'\n", appName );
        S = S + String.format( "\tspark log level .... '%s'\n", spark_log_level );
        S = S + String.format( "\tbatch_duration ..... %d seconds\n", batch_duration );
        S = S + String.format( "\tlocality.wait ...... %s\n", locality_wait );
        S = S + String.format( "\twith_locality ...... %s\n", Boolean.toString( with_locality ) );
        S = S + String.format( "\twith_dyn_alloc ..... %s\n", Boolean.toString( with_dyn_alloc ) );
        S = S + String.format( "\texecutors .......... %d ( %d cores )\n", num_executors, num_executor_cores );
        if ( !executor_memory.isEmpty() )
            S  =  S +  String.format( "\texecutor memory..... %s\n", executor_memory );

        S = S + "\nLOG:\n";
        S = S + String.format( "\tlog_host ........... %s:%d\n", log_host, log_port );
        String S_log = "";
        if ( log_request )   S_log = S_log + "request ";
        if ( log_job_start ) S_log = S_log + "job_start ";
        if ( log_job_done )  S_log = S_log + "job_done ";
        if ( log_cutoff )    S_log = S_log + "cutoff ";
        if ( log_final )     S_log = S_log + "final ";
        if ( log_part_prep )     S_log = S_log + "part-prep ";
        if ( log_worker_shift )  S_log = S_log + "worker-shift ";
        if ( log_pref_loc )  S_log = S_log + "pref_log ";
        if ( log_db_copy )   S_log = S_log + "db-copy ";

        S = S + String.format( "\tlog ................ %s\n", S_log );
        return S;
    }
 
}
