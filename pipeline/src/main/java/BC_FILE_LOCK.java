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

import java.io.File;
import java.io.FileWriter;
import java.io.BufferedWriter;

//import java.nio.channels.FileChannel;

public final class BC_FILE_LOCK
{
    private final String fileName;
    private File f;

    BC_FILE_LOCK( final String to_protect )
    {
        fileName = String.format( "%s.lock", to_protect );
        f = new File( fileName );
    }

    private boolean write_text( final String content )
    {
        boolean res = false;
        BufferedWriter writer = null;
        try
        {
            writer = new BufferedWriter( new FileWriter( f ) );
            writer.write( content );
        }
        catch( Exception e )
        {
            e.printStackTrace();
        }
        finally
        {
            try
            {
                writer.close();
                res = true;
            }
            catch( Exception e )
            {
                e.printStackTrace();
            }
        }
        return res;
    }

    public boolean aquire()
    {
        boolean res = false;
        if ( !f.exists() )
        {
            if ( write_text( "locked" ) )
            {
                return true;
            }
        }
        return res;
    }

    public void release()
    {
        if ( f.exists() ) f.delete();
    }

}
