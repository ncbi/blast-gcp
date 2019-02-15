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

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;

class SCRIPT_PLAYER extends Thread
{
    private final String script_filename;
    private final BLAST_STATUS status;

    public SCRIPT_PLAYER( final String a_script_filename, final BLAST_STATUS a_status )
    {
		this.script_filename = a_script_filename;
		this.status = a_status;
    }

    @Override public void run()
	{
        try
        {
			if ( !script_filename.isEmpty() )
			{
		        System.out.println( String.format( "running script: '%s'", script_filename ) );

		        FileInputStream fs = new FileInputStream( script_filename );
		        BufferedReader br = new BufferedReader( new InputStreamReader( fs ) );
		        String line;

		        while ( ( line = br.readLine() ) != null )
		        {
		            if ( !line.isEmpty() && !line.startsWith( "#" ) )
		            {
		                String cmd = line.trim();;
		                if ( !cmd.isEmpty() )
						{
		        			System.out.println( String.format( "script-line: '%s'", cmd ) );
							status.add_cmd( new CMD_Q_ENTRY( System.out, cmd ) );
		                }
		            }
		        }
		        br.close();
			}
		}
        catch( Exception e )
        {
            System.out.println( String.format( "SCRIPT_PLAYER : %s", e ) );
        }

	}
}
