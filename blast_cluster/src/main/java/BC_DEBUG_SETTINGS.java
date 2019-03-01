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

/**
 * Infrastructure-Class to store manage the debugging of the application
 * - stores host ( aka the master ) and port for a worker to connect to
 * - stores the log-level for the jni-interface to BLAST
 * - stores flags for different events to report via the debug-interface
*/
public class BC_DEBUG_SETTINGS implements Serializable
{
    public String host = "";
    public Integer port = 10011;
    public String jni_log_level = "INFO";
    public boolean request = false;
    public boolean job_start = false;
    public boolean job_done = false;
    public boolean log_final = false;
    public boolean part_prep = false;
    public boolean worker_shift = false;
    public boolean pref_loc = false;
    public boolean db_copy = false;
    public boolean req_file_added = false;
    public boolean req_added = false;
    public boolean avg_time = false;

/**
 * convert debug-settings to multiline string for debug-purpose
 *
 * @return     multiline string, printing all values of the debug-settings
*/
    @Override public String toString()
    {
        String S = String.format( "\tdebug_host ......... %s:%d\n", host, port );
        String S_log = "";
        if ( request )   S_log = S_log + "request ";
        if ( job_start ) S_log = S_log + "job_start ";
        if ( job_done )  S_log = S_log + "job_done ";
        if ( log_final )     S_log = S_log + "final ";
        if ( part_prep )     S_log = S_log + "part-prep ";
        if ( worker_shift )  S_log = S_log + "worker-shift ";
        if ( pref_loc )  S_log = S_log + "pref_log ";
        if ( db_copy )   S_log = S_log + "db-copy ";
        if ( req_added )   S_log = S_log + "req-file-add ";
        if ( req_file_added )   S_log = S_log + "req-add";
        if ( avg_time )   S_log = S_log + "avg-time";

        S = S + String.format( "\tlog ................ %s\n", S_log );

        return S;
    }
}

