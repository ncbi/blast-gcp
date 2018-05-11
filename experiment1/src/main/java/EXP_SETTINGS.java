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

import java.io.Serializable;

public class EXP_SETTINGS implements Serializable
{
    public String appName;
    
    public Integer batch_duration;
    public String  locality_wait;
    public Boolean with_locality;
    public Boolean with_dyn_alloc;
    public String  spark_log_level;

    public String log_host;
    public Integer log_port;
    
    public String trigger_host;
    public Integer trigger_port;

    public Integer num_partitions;
    public Integer num_executors;
    public Integer num_executor_cores;
    public String  executor_memory;

    public Boolean log_request;
    public Boolean log_cutoff;
    public Boolean log_final;
    public Boolean log_pref_loc;
    public Boolean log_part_prep;
    public Boolean log_prod1;
    public Boolean log_prod2;

    public Boolean valid()
    {
        if ( num_partitions == 0 ) return false;
        return true;
    }

    public String missing()
    {
        String S = "";
        if ( num_partitions == 0 ) S = S + "num_partitions is 0\n";
        return S;
    }

    @Override public String toString()
    {
        String S = String.format( "appName ............ '%s'\n", appName );
        S  =  S +  String.format( "spark log level .... '%s'\n", spark_log_level );
        S  =  S +  String.format( "batch_duration ..... %d seconds\n", batch_duration );
        S  =  S +  String.format( "locality.wait ...... %s\n", locality_wait );
        S  =  S +  String.format( "with_locality ...... %s\n", Boolean.toString( with_locality ) );
        S  =  S +  String.format( "with_dyn_alloc ..... %s\n", Boolean.toString( with_dyn_alloc ) );
        S  =  S +  String.format( "log_host ........... %s:%d\n", log_host, log_port );
        S  =  S +  String.format( "trigger_host ....... %s:%d\n", trigger_host, trigger_port );
        S  =  S +  String.format( "num_partitions ..... %d\n", num_partitions );
        S  =  S +  String.format( "executors .......... %d ( %d cores )\n", num_executors, num_executor_cores );
        if ( !executor_memory.isEmpty() )
            S  =  S +  String.format( "executor memory..... %s\n", executor_memory );

        String S_log = "";
        if ( log_request )   S_log = S_log + "request ";
        if ( log_cutoff )    S_log = S_log + "cutoff ";
        if ( log_final )     S_log = S_log + "final ";
        if ( log_pref_loc )  S_log = S_log + "pref-loc ";
        if ( log_part_prep ) S_log = S_log + "part-prep ";
        if ( log_prod1 )     S_log = S_log + "prod1 ";
        if ( log_prod2 )     S_log = S_log + "prod2 ";

        S  =  S +  String.format( "log ................ %s\n", S_log );

        return S;
    }
 
}
