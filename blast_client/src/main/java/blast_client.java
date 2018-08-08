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

package gov.nih.nlm.ncbi.blast_client;

public final class blast_client
{
    public static void main( String[] args )
    {
        if ( args.length < 3 )
            System.out.println( "port-number, request-list, and db_location are missing" );
        else
        {
			String executable = "./blast_server";
			int port = Integer.parseInt( args[ 0 ].trim() );
			int num_threads = 3;
			String request_list_path = args[ 1 ];
			String db_locations = args[ 2 ];

			blast_runner_2.run( executable, port, num_threads, request_list_path, db_locations );
        }
   }

}
