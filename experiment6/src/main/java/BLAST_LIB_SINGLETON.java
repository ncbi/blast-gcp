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

import java.util.Map;
import java.util.List;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;

import java.io.File;
import java.nio.file.Paths;
import java.nio.file.Files;

import com.google.api.services.storage.Storage;

class PART_INST
{
    public Integer part_nr;
    public String key;
    public Long size;
    public Boolean prepared;

    public PART_INST( final BLAST_DATABASE_PART part )
    {
        part_nr = part.nr;
        key = part.volume.key;
        size = 0L;
        prepared = false;
    }

	/*
	private Boolean protected_download( final Storage storage, final String bucket,
									    final String src_file, final String dst_file,
										final BLAST_LOG_SETTING log )
	{
        if ( log.db_copy )
            BLAST_SEND.send( log, String.format( "'%s:%s' --> '%s'", bucket, src_file, dst_file ) );

        return BLAST_GS_DOWNLOADER.download( storage, bucket, src_file, dst_file );
	}
	*/

    public Boolean prepare( final BLAST_DATABASE_PART part, final BLAST_LOG_SETTING log )
    {
        Boolean res = false;
        try
        {
            List< CONF_VOLUME_FILE > to_copy = new LinkedList<>();

            for ( CONF_VOLUME_FILE vf : part.volume.files )
            {
                if ( !vf.present() )
                    to_copy.add( vf );
            }

            if ( !to_copy.isEmpty() )
            {
                int copied = 0;
                Storage storage = BLAST_GS_DOWNLOADER.buildStorageService();

                for ( CONF_VOLUME_FILE vf : to_copy )
                {
					copied += vf.copy( storage, part.volume.bucket, log );
                }
                res = ( copied == to_copy.size() );
            }
            else
                res = true;

            if ( size == 0L )
            {
                for ( CONF_VOLUME_FILE vf : part.volume.files )
                {
                    File f = new File( vf.f_local );
                    size += f.length();
                }
            }
        }
        catch( Exception e )
        {
            BLAST_SEND.send( log, String.format( "gs: %s", e ) );
        }
        return res;
    }
}

class PART_MAP
{
    public static Map< Integer, PART_INST > map = new ConcurrentHashMap<>();
}

class BLAST_LIB_SINGLETON
{
    // let us have a map of PARTITION-ID to BLAST_LIB
    private static Map< String, PART_MAP > map = new ConcurrentHashMap<>();
    private static BLAST_LIB blaster = null;

    /* this ensures that nobody can make an instance of this class, but the class itself */
    private BLAST_LIB_SINGLETON()
    {
    }

    private static PART_INST getPartInst( final BLAST_DATABASE_PART part )
    {
        PART_INST res;
        if ( map.containsKey( part.volume.key ) )
        {
            PART_MAP m = map.get( part.volume.key );
            if ( m.map.containsKey( part.nr ) )
                res = m.map.get( part.nr );
            else
            {
                res = new PART_INST( part );
                m.map.put( part.nr, res );
            }
        }
        else
        {
            res = new PART_INST( part );
            PART_MAP m = new PART_MAP();
            m.map.put( part.nr, res );
            map.put( part.volume.key, m );
        }
        return res;
    }

    public static BLAST_DATABASE_PART prepare( final BLAST_DATABASE_PART part, final BLAST_LOG_SETTING log )
    {
        PART_INST p_inst = getPartInst( part );
        if ( p_inst != null )
        {
            if ( !p_inst.prepared )
                p_inst.prepared = p_inst.prepare( part, log );
            
            return part.enter_worker_name();
        }
        return null;
    }

    public static BLAST_LIB get_lib( final String libname, final BLAST_DATABASE_PART part, final BLAST_LOG_SETTING log )
    {
        PART_INST p_inst = getPartInst( part );
        if ( p_inst != null )
        {
            if ( !p_inst.prepared )
                p_inst.prepared = p_inst.prepare( part, log );
        }
        if ( blaster == null )
            blaster = new BLAST_LIB( libname );
        return blaster;
    }

    public static Long get_size( final BLAST_DATABASE_PART part )
    {
        PART_INST p_inst = getPartInst( part );
        if ( p_inst != null )
            return p_inst.size;
        return 0L;
    }

}

