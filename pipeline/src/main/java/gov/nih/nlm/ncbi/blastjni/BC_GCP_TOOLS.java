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

import java.util.List;
import java.util.ArrayList;

import java.io.File;
import java.io.FileWriter;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ByteArrayInputStream;

import java.net.URI;
import java.net.URISyntaxException;

import java.security.GeneralSecurityException;
import java.nio.ByteBuffer;
import java.util.Collection;

import com.google.cloud.storage.Bucket;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.InputStreamContent;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.StorageScopes;
import com.google.api.services.storage.model.Objects;
import com.google.api.services.storage.model.StorageObject;

class BC_GCP_TOOLS
{
    private static BC_GCP_TOOLS instance = null;
    private Storage storage = null;

    public static Storage buildStorageService( final String AppName ) throws GeneralSecurityException, IOException
    {
        HttpTransport transport = GoogleNetHttpTransport.newTrustedTransport();
        JsonFactory jsonFactory = new JacksonFactory();
        GoogleCredential credential = GoogleCredential.getApplicationDefault( transport, jsonFactory );

        if ( credential.createScopedRequired() )
        {
            Collection<String> scopes = StorageScopes.all();
            credential = credential.createScoped( scopes );
        }
        return new Storage.Builder( transport, jsonFactory, credential ).setApplicationName( AppName ).build();
    }

	/* constructor made private to prevent instantiation, this class is a singleton! */
    private BC_GCP_TOOLS( final String AppName )
    {
        try
        {
            storage = buildStorageService( AppName );
        }
        catch ( Exception e )
        {
            storage = null;
        }
    }

    public static BC_GCP_TOOLS getInstance()
    {
        if ( instance == null )
        {
            instance = new BC_GCP_TOOLS( "BlastSparkCluster" );
        }
        return instance;
    }

    private Integer uploadContent( final String bucket, final String key, final ByteBuffer content )
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
            Storage.Objects.Insert insertRequest = storage.objects().insert( bucket, objectMetadata, contentStream );

            insertRequest.execute();
        }
        catch ( Exception e )
        {
            res = 0;
        }
        return res;
    }

	private Integer list_bucket( final String bucket, List< String > lst )
	{
		Integer res = 0; 
        try
        {
            Storage.Objects.List list = storage.objects().list( bucket );
            if ( list != null )
            {
				Objects objects;
				do
				{
					objects = list.execute();
					List< StorageObject > items = objects.getItems();
					for ( StorageObject item : items )
					{
						lst.add( item.getName() );
						res += 1;
					}
					list.setPageToken( objects.getNextPageToken() );
				} while ( objects.getNextPageToken() != null );
            }
        }
        catch( Exception e )
        {
        }
		return res;
	}

	private void write_lock( File f )
	{
		BufferedWriter writer = null;
		try
		{
			writer = new BufferedWriter( new FileWriter( f ) );
			writer.write( "locked" );
		}
		catch( Exception e ) { e.printStackTrace(); }	
		finally { try{ writer.close(); } catch( Exception e ) { e.printStackTrace(); } }
	}

	private void delete_lock( File f )
	{
		try	{ f.delete(); }	
		catch( Exception e ) { e.printStackTrace(); }
	}

    private boolean download_to_file( final String bucket, final String key, final String dst_filename )
    {
        boolean res = BC_UTILS.create_paths_if_neccessary( dst_filename );
		if ( res )
		{
			File f_lock = new File( String.format( "%s.lock", dst_filename ) );
			res = ( !f_lock.exists() );
			if ( res )
			{
				write_lock( f_lock );
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
				    }
					else
						res = false;
				}
				catch( Exception e ) { res = false; }
				finally { delete_lock( f_lock ); }
			}

		}
        return res;
    }

    public static InputStream download_as_stream( final String bucket, final String key )
    {
        InputStream res = null;
        BC_GCP_TOOLS inst = getInstance();
        if ( inst != null )
		{
		    try
		    {
		        Storage.Objects.Get obj = inst.storage.objects().get( bucket, key );
		        if ( obj != null )
		        {
		            obj.getMediaHttpDownloader().setDirectDownloadEnabled( true );
		            return obj.executeMediaAsInputStream(); 
		        }
		    }
		    catch( Exception e )
		    {
		    }
		}
        return res;
    }

    public static boolean download( final String gs_uri, final String dst_filename )
    {
        boolean res = false;
        BC_GCP_TOOLS inst = getInstance();
        if ( inst != null )
		{
		    try
		    {
		        URI uri = new URI( gs_uri );
		        if ( uri.getScheme().equals( "gs" ) )
		        {
		            String bucket = uri.getAuthority();
		            String key = uri.getPath();
		            if ( key.startsWith( "/" ) )
		                key = key.substring( 1 );
		            return inst.download_to_file( bucket, key, dst_filename );
		        }
		    }
		    catch( URISyntaxException e )
		    {
		    }
		}
        return res;
    }

    public static InputStream download( final String gs_uri )
    {
        InputStream res = null;
        BC_GCP_TOOLS inst = getInstance();
        if ( inst != null )
		{
		    try
		    {
		        URI uri = new URI( gs_uri );
		        if ( uri.getScheme().equals( "gs" ) )
		        {
		            String bucket = uri.getAuthority();
		            String key = uri.getPath();
		            if ( key.startsWith( "/" ) )
		                key = key.substring( 1 );
		            return inst.download_as_stream( bucket, key );
		        }
		    }
		    catch( URISyntaxException e )
		    {
		    }
		}
        return res;
    }

    public static Integer upload( final String bucket, final String key, final ByteBuffer content )
    {
        Integer res = 0;
        BC_GCP_TOOLS inst = getInstance();
        if ( inst != null )
            res = inst.uploadContent( bucket, key, content );
        return res;
    }

    public static Integer upload( final String bucket, final String key, final String content )
    {
        ByteBuffer bb = ByteBuffer.wrap( content.getBytes() );
        return upload( bucket, key, bb );
    }

	public static List< String > list( final String gs_uri )
	{
		List< String > res = new ArrayList<>();
        BC_GCP_TOOLS inst = getInstance();
        if ( inst != null )
		{
		    try
		    {
		        URI uri = new URI( gs_uri );
		        if ( uri.getScheme().equals( "gs" ) )
		        {
		            String bucket = uri.getAuthority();
					inst.list_bucket( bucket, res );
		        }
		    }
		    catch( URISyntaxException e )
		    {
		    }
		}
		return res;
	}

	private static boolean ends_with_any( final String s, final List< String > extensions )
	{
		for ( String ext : extensions )
		{
			if ( s.endsWith( ext ) ) return true;
		}
		return false;
	}

	public static List< String > unique_without_extension( final List< String > all, final List< String > extensions )
	{
		List< String > res = new ArrayList<>();
		for ( String s : all )
		{
			if ( ends_with_any( s, extensions ) )
			{
				String without_ext = s.substring( 0, s.length() - 4 );
				if ( ! res.contains( without_ext ) )
					res.add( without_ext );
			}
		}
		return res;
	}

}
