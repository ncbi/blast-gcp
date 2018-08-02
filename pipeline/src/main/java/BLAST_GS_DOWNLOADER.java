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
import java.io.FileOutputStream;
import java.net.URI;
import java.net.URISyntaxException;

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
import java.io.InputStream;
import java.util.Collection;

class BLAST_GS_DOWNLOADER
{
    public static Storage buildStorageService() throws GeneralSecurityException, IOException
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

    public static Boolean download( Storage storage, final String bucket, final String key, final String dst_filename )
    {
        Boolean res = false;
        try
        {
            Storage.Objects.Get obj = storage.objects().get( bucket, key );
            if ( obj != null )
            {
                obj.getMediaHttpDownloader().setDirectDownloadEnabled( true );

                File f = new File( dst_filename );
                FileOutputStream f_out = new FileOutputStream( f );

                obj.executeMediaAndDownloadTo( f_out );

                f_out.flush();
                f_out.close();
                res = true;
            }
        }
        catch( Exception e )
        {
        }
        return res;
    }

    public static InputStream download_as_stream( Storage storage, final String bucket, final String key )
    {
        InputStream res = null;
        try
        {
            Storage.Objects.Get obj = storage.objects().get( bucket, key );
            if ( obj != null )
            {
                obj.getMediaHttpDownloader().setDirectDownloadEnabled( true );
                return obj.executeMediaAsInputStream(); 
            }
        }
        catch( Exception e )
        {
        }
        return res;

    }

    public static Boolean download_uri( Storage storage, final String gs_url, final String dst_filename )
    {
        Boolean res = false;
        try
        {
            URI uri = new URI( gs_url );
            if ( uri.getScheme().equals( "gs" ) )
            {
                String bucket = uri.getAuthority();
                String key = uri.getPath();
                if ( key.startsWith( "/" ) )
                    key = key.substring( 1 );
                return download( storage, bucket, key, dst_filename );
            }
        }
        catch( URISyntaxException e )
        {
        }
        return res;
    }

    public static InputStream download_uri_as_stream( Storage storage, final String gs_url )
    {
        InputStream res = null;
        try
        {
            URI uri = new URI( gs_url );
            if ( uri.getScheme().equals( "gs" ) )
            {
                String bucket = uri.getAuthority();
                String key = uri.getPath();
                if ( key.startsWith( "/" ) )
                    key = key.substring( 1 );
                return download_as_stream( storage, bucket, key );
            }
        }
        catch( URISyntaxException e )
        {
        }
        return res;
    }

}

