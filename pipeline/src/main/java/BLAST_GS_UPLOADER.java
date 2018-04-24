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

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.InputStreamContent;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.StorageScopes;
import com.google.api.services.storage.model.StorageObject;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.util.Collection;

public class BLAST_GS_UPLOADER
{
    private static BLAST_GS_UPLOADER instance = null;

    private final String bucket;
    private Storage storage;

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

    public static BLAST_GS_UPLOADER getInstance( final String bucket )
    {
        if ( instance == null )
        {
            instance = new BLAST_GS_UPLOADER( bucket );
        }
        return instance;
    }

    private Integer uploadFile( String key, ByteBuffer content )
    {
        Integer res = 0;
        try
        {
            String mime_type = "application/octet-stream"; /* "text/plain" */
            ByteArrayInputStream bytes = new ByteArrayInputStream( content.array() );
            InputStreamContent contentStream = new InputStreamContent( mime_type, bytes );
            // Setting the length improves upload performance
            res = content.array().length;
            contentStream.setLength( res );

            // Destination object name
            StorageObject objectMetadata = new StorageObject().setName( key );

            // Do the insert
            Storage.Objects.Insert insertRequest = storage.objects().insert( this.bucket, objectMetadata, contentStream );

            insertRequest.execute();
        }
        catch ( Exception e )
        {
            res = 0;
        }
        return res;
    }

    private BLAST_GS_UPLOADER( final String bucket )
    {
        this.bucket = bucket;
        try
        {
            this.storage = buildStorageService();
        }
        catch ( Exception e )
        {
            this.storage = null;
        }
    }

    public static Integer upload( final String bucket, final String key, final ByteBuffer content )
    {
        Integer res = 0;
        BLAST_GS_UPLOADER inst = getInstance( bucket );
        if ( inst != null )
            res = inst.uploadFile( key, content );
        return res;
    }

    public static Integer upload( final String bucket, final String key, final String content )
    {
        ByteBuffer bb = ByteBuffer.wrap( content.getBytes() );
        return upload( bucket, key, bb );
    }

}
