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
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * utility-class to provide a file-system-lock between different
 * jwm-instances, using File.mkdir() which is atomic in the file-system
 *
*/
public final class BC_FILE_LOCK implements AutoCloseable
{
    private final AtomicBoolean locked;
    private File f;

/**
 * consructor, to create instance of FILE_LOCK
 *
 * @param   to_protect  file to protect from multiple access
*/
    BC_FILE_LOCK( final String to_protect )
    {
        locked = new AtomicBoolean( false );
        f = new File( String.format( "%s.lock", to_protect ) );
        String parent = f.getParent();
        if ( parent != null )
        {
            File p = new File( parent );
            p.mkdirs();
        }
        try
        {
            /*  mkdir is atomic at the filesystem-level! */
            locked.set( f.mkdir() );
        }
        catch( Exception e )
        {
            e.printStackTrace();
        }
    }

/**
 * aquire the lock: we are creating a directory!
 *
 * @return creating the directory was successful, we have the lock!
*/
    public boolean locked()
    {
        return locked.get();
    }

/**
 * releaseing the lock: we delete the directory!
 *
*/
    @Override public void close()
    {
        if ( locked.get() )
        {
            try
            {
                f.delete();
                locked.set( false );
            }
            catch( Exception e )
            {
                e.printStackTrace();
            }
        }
    }

}
