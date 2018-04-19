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
import java.nio.file.Path;
import java.nio.file.Paths;

/*
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.ServiceOptions;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.storage.StorageOptions;
*/

import com.google.cloud.storage.Bucket;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.InputStreamContent;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.StorageScopes;
import com.google.api.services.storage.model.StorageObject;

import java.security.GeneralSecurityException;
import java.io.IOException;
import java.util.Collection;

class PART_INST
{
    public BLAST_LIB blaster;
    public Integer part_id, requested, size;
    public Boolean prepared;

    public PART_INST( final BLAST_PARTITION part )
    {
        blaster = new BLAST_LIB();
        part_id = part.nr;
        requested = 0;
        size = 0;
        prepared = false;
    }

    private static Storage buildStorageService() throws GeneralSecurityException, IOException
    {
        HttpTransport transport = GoogleNetHttpTransport.newTrustedTransport();
        JsonFactory jsonFactory = new JacksonFactory();
        GoogleCredential credential = GoogleCredential.getApplicationDefault( transport, jsonFactory );

        if ( credential.createScopedRequired() )
        {
            Collection<String> scopes = StorageScopes.all();
            credential = credential.createScoped( scopes );
        }
        return new Storage.Builder( transport, jsonFactory, credential ).build();
    }

    public BLAST_PARTITION prepare( final BLAST_PARTITION part, final BLAST_SETTINGS settings )
    {
        /*
        try
        {
            List< String > extensions = new LinkedList<>();
            extensions.add( "nhr" );
            extensions.add( "nin" );
            extensions.add( "nsq" );

            List< String > obj_names = new LinkedList<>();
            for ( String ext : extensions )
            {
                String fn = String.format( "%s.%s", part.db_spec, ext );
                File f = new File( fn );
                if ( !f.exists() )
                    obj_names.add( String.format( "%s.%s", part.name, ext ) );
            }
            if ( !obj_names.isEmpty() )
            {
                Storage storage = buildStorageService();
                for ( String obj_name : obj_names )
                {
                    Storage.Objects.Get obj = storage.objects().get( settings.db_bucket, obj_name );
                    if ( obj != null )
                    {
                        getObject.getMediaHttpDownloader().setDirectDownloadEnabled( !IS_APP_ENGINE );

                        String dst_fn;

                        if ( settings.flat_db_layout )
                            dst_fn = String.format( "%s/%s", settings.db_location, obj_name );
                        else
                            dst_fn = String.format( "%s/%s/%s", settings.db_location, part.name, obj_name );

                        BLAST_SEND.send( settings, String.format( "'%s:%s' --> '%s'", settings.db_bucket, obj_name, dst_fn ) );

                        File f = new File( dst_fn );
                        FileOutputStream f_out = new FileOutputStream( f );

                        ByteArrayOutputStream out = new ByteArrayOutputStream();
                        obj.executeMediaAndDownloadTo( out );

                    }
                }
            }
        }
        catch( Exception e )
        {
            BLAST_SEND.send( settings, String.format( "gs: %", e ) );
        }
        */
        prepared = true;
        return part.prepare();
    }

    public BLAST_LIB get_lib( final BLAST_PARTITION part )
    {
        return blaster;
    }
}

class BLAST_LIB_SINGLETON
{
    // let us have a map of PARTITION-ID to BLAST_LIB
    private static Map< Integer, PART_INST > parts = new ConcurrentHashMap<>();

    /* this ensures that nobody can make an instance of this class, but the class itself */
    private BLAST_LIB_SINGLETON()
    {
    }

    private static PART_INST getInstance( final BLAST_PARTITION part )
    {
        if ( !parts.containsKey( part.nr ) )
            parts.put( part.nr, new PART_INST( part ) );
        return parts.get( part.nr );
    }

    public static BLAST_PARTITION prepare( final BLAST_PARTITION part, final BLAST_SETTINGS settings )
    {
        PART_INST inst = getInstance( part );
        if ( inst != null )
        {
            inst.requested += 1;
            return inst.prepare( part, settings );
        }
        return null;
    }

    public static BLAST_LIB get_lib( final BLAST_PARTITION part )
    {
        PART_INST inst = getInstance( part );
        if ( inst != null )
        {
            inst.requested += 1;            
            return inst.get_lib( part );
        }
        return null;
    }

    public static Integer get_requests( final BLAST_PARTITION part )
    {
        PART_INST inst = getInstance( part );
        if ( inst != null )
            return inst.requested;
        return 0;
    }
}

