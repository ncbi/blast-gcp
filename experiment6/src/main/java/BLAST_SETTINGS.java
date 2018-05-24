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
    BLAST_DB_SETTINGS db_list;

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

    /* SPARK */
    public List< String > transfer_files;
    public String  spark_log_level;
    public String  locality_wait;
    public Boolean with_locality;
    public Boolean with_dyn_alloc;
    public Integer num_executors;
    public Integer num_executor_cores;
    public String  executor_memory;
    public Boolean shuffle_reduceLocality_enabled;
    public Boolean scheduler_fair;
    public Integer parallel_jobs;
    public Integer comm_port;

    /* LOG */
    BLAST_LOG_SETTING log;

    public BLAST_SETTINGS()
    {
        db_list = new BLAST_DB_SETTINGS();
        log = new BLAST_LOG_SETTING();
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

    public Boolean valid()
    {
        if ( !db_list.valid() ) return false;
        if ( top_n == 0 ) return false;

        if ( !src_valid() ) return false;

        if ( gs_result_bucket.isEmpty() ) return false;
        if ( gs_status_bucket.isEmpty() ) return false;
        return true;
    }

    public String missing()
    {
        String S = db_list.missing();
        if ( top_n == 0 ) S = S + "top_n is 0\n";

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

        if ( gs_result_bucket.isEmpty() ) S = S + "gs_result_bucket is missing\n";
        if ( gs_status_bucket.isEmpty() ) S = S + "gs_status_bucket is missing\n";
        return S;
    }

    @Override public String toString()
    {
        String S = "SOURCE:\n";
        if ( use_pubsub_source )
            S = S + String.format( "\tpubsub-subscript ... '%s' : '%s'\n", project_id, subscript_id );
        if ( use_hdfs_source )
            S = S + String.format( "\tHDFS-dir ........... '%s'\n", hdfs_source_dir );
        S = S + String.format( "\tmax. backlog ........... %d requests\n", max_backlog );

        S = S + "\nDB:\n" + db_list.toString();

        S = S + "\nBLASTJNI:\n";
        S = S + String.format( "\tdflt top_n ......... %d\n", top_n );
        S = S + String.format( "\tjni-log-level ...... %s\n", jni_log_level );

        S = S + "\nRESULTS:\n";
        S = S + String.format( "\tGS result bucket ... '%s'\n", gs_result_bucket );
        S = S + String.format( "\tGS status bucket ... '%s'\n", gs_status_bucket);        
        S = S + String.format( "\tHDFS result ........ '%s%s'\n", hdfs_result_dir, hdfs_result_file );
        S = S + String.format( "\tGS OR HDFS ......... '%s'\n", gs_or_hdfs );
        

        S = S + "\nSPARK:\n";
        S = S + String.format( "\tappName ............ '%s'\n", appName );
        S = S + String.format( "\ttransfer files ..... %s\n", transfer_files );
        S = S + String.format( "\tspark log level .... '%s'\n", spark_log_level );
        S = S + String.format( "\tlocality.wait ...... %s\n", locality_wait );
        S = S + String.format( "\twith_locality ...... %s\n", Boolean.toString( with_locality ) );
        S = S + String.format( "\twith_dyn_alloc ..... %s\n", Boolean.toString( with_dyn_alloc ) );
        S = S + String.format( "\texecutors .......... %d ( %d cores )\n", num_executors, num_executor_cores );
        S = S + String.format( "\treduce_loc_enabled . %s\n", Boolean.toString( shuffle_reduceLocality_enabled ) );
        S = S + String.format( "\tscheduler fair ..... %s\n", Boolean.toString( scheduler_fair ) );
        S = S + String.format( "\tparallel jobs ...... %d\n", parallel_jobs );
        S = S + String.format( "\tcommunication port . %d\n", comm_port );
        if ( !executor_memory.isEmpty() )
            S  =  S +  String.format( "\texecutor memory..... %s\n", executor_memory );

        S = S + log.toString();
        return S;
    }
 
}
