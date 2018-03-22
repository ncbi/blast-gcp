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

import java.util.*;
import java.io.*;
import java.net.*;

public class GCP_BLAST_SETTINGS
{
    public String appName;
    public Integer batch_duration;
    public List< String > files_to_transfer;
    public String log_host;
    public Integer log_port;
    public String trigger_dir;
    public String save_dir;
    public Integer num_db_partitions;
    public Integer num_job_partitions;
    
    public GCP_BLAST_SETTINGS( final String appName )
    {
        this.appName = appName;
        batch_duration = 1;
        files_to_transfer = new ArrayList<>();
        
        try
        {
            java.net.InetAddress localMachine = java.net.InetAddress.getLocalHost();
            log_host = localMachine.getHostName();
        }
        catch ( UnknownHostException e )
        {
            System.out.println( String.format( "cannot detect name of local machine: %s", e ) );
            log_host = "";
        }
        log_port = 10011;
        
        final String username = System.getProperty( "user.name" );
        trigger_dir = String.format( "hdfs:///user/%s/todo/", username );
        save_dir = String.format( "hdfs:///user/%s/results/", username );
        
        num_db_partitions = 10;
        num_job_partitions = 1;
    }
}
