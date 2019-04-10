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
import java.util.HashMap;
import java.util.Map;

import java.io.File;
import java.io.FileWriter;
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

/**
 * utility-class to read from / write to GCP-buckets
 * - has a static instance of itself, to function as singleton
 *
*/
public class BC_GCP_TOOLS
{
    private static BC_GCP_TOOLS instance = null;
    private Storage storage = null;

/**
 * private helper-function to create Storage-instance, needed for access to buckets
 *
 * @param AppName  name of the application to be given to the storage-instance
 * @return         Storage-instance
*/
    private static Storage buildStorageService( final String AppName ) throws GeneralSecurityException, IOException
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

/**
 * private constructor to prevent accidental instantiation
 *
 * @param AppName  name of the application to be given to the storage-instance
*/
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

/**
 * static method to either return an existing instance or create a new instance
 * of the BC_GCP_TOOLS-class
 *
 * @return         instance of BC_GCP_TOOLS
*/
    public static BC_GCP_TOOLS getInstance()
    {
        if ( instance == null )
        {
            instance = new BC_GCP_TOOLS( "BlastSparkCluster" );
        }
        return instance;
    }

/**
 * private method to upload a ByteBuffer into a bucket
 *
 * @param  bucket  url of the bucket
 * @param  key     name of the 'file' in the bucket to be created or overwritten
 * @param  content bytes to be uploaded
 *
 * @return         number of bytes uploaded
*/
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

/**
 * private method list all entries of a bucket : name + size
 *
 * @param  bucket  url of the bucket
 * @param  lst     reference of list of string/size-tuples, to insert entry-names into
 *
 * @return         number of entries found
*/
    private Integer list_bucket( final String bucket, List< BC_NAME_SIZE > lst )
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
                        lst.add( new BC_NAME_SIZE( item.getName(), item.getSize() ) );
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

/**
 * private method list all entries of a bucket: just names
 *
 * @param  bucket  url of the bucket
 * @param  lst     reference of list of strings, to insert entry-names into
 *
 * @return         number of entries found
*/
    private Integer list_bucket_names( final String bucket, List< String > lst, int limit, final String filter )
    {
        Integer res = 0;
        try
        {
            Storage.Objects.List list = storage.objects().list( bucket );
            if ( list != null )
            {
                Objects objects;
                if ( limit > 0 )
                {
                    boolean done = false;
                    do
                    {
                        objects = list.execute();
                        List< StorageObject > items = objects.getItems();
                        for ( StorageObject item : items )
                        {
                            String s = item.getName();
                            boolean valid = true;
                            if ( !filter.isEmpty() ) valid = s.endsWith( filter );
                            if ( valid )
                            {
                                lst.add( item.getName() );
                                res += 1;
                                done = ( res >= limit );
                                if ( done ) break;
                            }
                        }
                        list.setPageToken( objects.getNextPageToken() );
                    } while ( !done && ( objects.getNextPageToken() != null ) );
                }
                else
                {
                    do
                    {
                        objects = list.execute();
                        List< StorageObject > items = objects.getItems();
                        for ( StorageObject item : items )
                        {
                            String s = item.getName();
                            boolean valid = true;
                            if ( !filter.isEmpty() ) valid = s.endsWith( filter );
                            if ( valid )
                            {
                                lst.add( item.getName() );
                                res += 1;
                            }
                        }
                        list.setPageToken( objects.getNextPageToken() );
                    } while ( objects.getNextPageToken() != null );
                }
            }
        }
        catch( Exception e )
        {
        }
        return res;
    }

/**
 * private method to download a file from a bucket to the local filesystem, protected by a lock-file
 *
 * @param  bucket        url of the bucket
 * @param  key           name of the 'file' in the bucket to be downloaded
 * @param  dst_filename  absolute path of destination-file to be created
 *
 * return  success of operation
*/
    private boolean download_to_file( final String bucket, final String key, final String dst_filename )
    {
        boolean res = true;
        if ( res )
        {
            try
            {
                Storage.Objects.Get obj = storage.objects().get( bucket, key );
                res = ( obj != null );
                if ( res )
                {

                    File f = new File( dst_filename );
                    FileOutputStream f_out = new FileOutputStream( f );

                    try
                    {
                        try {
                            obj.executeMediaAndDownloadTo( f_out );
                        }
                        catch( Exception e ) {
                            e.printStackTrace();
                            res = false;
                        }
                    }
                    catch( Exception e )
                    {
                        e.printStackTrace();
                        res = false;
                    }
                    finally
                    {
                        f_out.flush();
                        f_out.close();
                    }
                    if ( !res )
                    {
                        try
                        {
                            if ( f.exists() )
                                f.delete();
                        }
                        catch( Exception e )
                        {
                            e.printStackTrace();
                        }
                    }
                }
            }
            catch( Exception e )
            {
                e.printStackTrace();
                res = false;
            }
        }
        return res;
    }

/**
 * public static method to download a file from a bucket as stream
 *
 * @param  bucket        url of the bucket
 * @param  key           name of the 'file' in the bucket to be downloaded
 *
 * return  InputStream to be read from
*/
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

/**
 * public static method to download a file from a bucket to the local filesystem
 *
 * @param  bucket        url of the bucket
 * @param  dst_filename  absolute path of destination-file to be created
 *
 * return  success of operation
*/
    public static boolean download( final String bucket, final String dst_filename )
    {
        boolean res = false;
        BC_GCP_TOOLS inst = getInstance();
        if ( inst != null )
        {
            try
            {
                URI uri = new URI( bucket );
                if ( uri.getScheme().equals( "gs" ) )
                {
                    String key = uri.getPath();
                    if ( key.startsWith( "/" ) )
                        key = key.substring( 1 );
                    return inst.download_to_file( uri.getAuthority(), key, dst_filename );
                }
            }
            catch( URISyntaxException e )
            {
            }
        }
        return res;
    }

/**
 * public static method to download a bucket-url as stream
 *
 * @param  bucket        url of the bucket-item
 *
 * return  InputStream to be read from
*/
    public static InputStream download( final String bucket )
    {
        InputStream res = null;
        BC_GCP_TOOLS inst = getInstance();
        if ( inst != null )
        {
            try
            {
                URI uri = new URI( bucket );
                if ( uri.getScheme().equals( "gs" ) )
                {
                    String key = uri.getPath();
                    if ( key.startsWith( "/" ) )
                        key = key.substring( 1 );
                    return inst.download_as_stream( uri.getAuthority(), key );
                }
            }
            catch( URISyntaxException e )
            {
            }
        }
        return res;
    }

/**
 * public static method to upload a ByteBuffer into a bucket
 *
 * @param  bucket  url of the bucket
 * @param  key     name of the 'file' in the bucket to be created or overwritten
 * @param  content bytes to be uploaded
 *
 * @return         number of bytes uploaded
*/
    public static Integer upload( final String bucket, final String key, final ByteBuffer content )
    {
        Integer res = 0;
        BC_GCP_TOOLS inst = getInstance();
        if ( inst != null )
            res = inst.uploadContent( bucket, key, content );
        return res;
    }

/**
 * public static method to upload a String into a bucket
 *
 * @param  bucket  url of the bucket
 * @param  key     name of the 'file' in the bucket to be created or overwritten
 * @param  content String to be uploaded
 *
 * @return         number of bytes uploaded
*/
    public static Integer upload( final String bucket, final String key, final String content )
    {
        ByteBuffer bb = ByteBuffer.wrap( content.getBytes() );
        return upload( bucket, key, bb );
    }

/**
 * public static method to list all items in a bucket
 *
 * @param  bucket  url of the bucket
 *
 * @return         List of String/Size-tuples, names/size of items in the bucket
*/
    public static List< BC_NAME_SIZE > list( final String bucket )
    {
        List< BC_NAME_SIZE  > res = new ArrayList<>();
        BC_GCP_TOOLS inst = getInstance();
        if ( inst != null )
        {
            try
            {
                URI uri = new URI( bucket );
                if ( uri.getScheme().equals( "gs" ) )
                    inst.list_bucket( uri.getAuthority(), res );
            }
            catch( URISyntaxException e )
            {
            }
        }
        return res;
    }

/**
 * public static method to list all items in a bucket
 *
 * @param  bucket  url of the bucket
 *
 * @return         List of Strings, names of items in the bucket
*/
    public static List< String > list_names( final String bucket, int limit, final String filter )
    {
        List< String  > res = new ArrayList<>();
        BC_GCP_TOOLS inst = getInstance();
        if ( inst != null )
        {
            try
            {
                URI uri = new URI( bucket );
                if ( uri.getScheme().equals( "gs" ) )
                    inst.list_bucket_names( uri.getAuthority(), res, limit, filter );
            }
            catch( URISyntaxException e )
            {
            }
        }
        return res;
    }

/**
 * private static helper-method to test if a String ends in any of the given extensions
 *
 * @param  s           string to be tested
 * @param  extensions  extensions to be probed
 *
 * @return         does the string end in any of the given extensions ?
*/
    private static boolean ends_with_any( final String s, final List< String > extensions )
    {
        for ( String ext : extensions )
        {
            if ( s.endsWith( ext ) ) return true;
        }
        return false;
    }

/**
 * public static helper-method to filter a list of Strings, based on a list of given extensions
 *
 * @param  all         list of strings to be filtered
 * @param  extensions  extensions to be used as filter
 *
 * @return         list of strings that do end in any of the given extensions
*/
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

/**
 * public static helper-method to filter a list of BC_NAME_SIZE tuples, based on a list of given extensions
 *
 * @param  all         list of BC_NAME_SIZE tuples
 * @param  extensions  extensions to be used as filter
 *
 * @return         list of strings that do end in any of the given extensions
*/
    public static List< BC_CHUNK_VALUES > unique_by_extension( final List< BC_NAME_SIZE > all, final List< String > extensions )
    {
        List< BC_CHUNK_VALUES > res = new ArrayList<>();
        Map< String, BC_CHUNK_VALUES > dict = new HashMap<>();
        for ( BC_NAME_SIZE item : all )
        {
            if ( ends_with_any( item.name, extensions ) )
            {
                String key = item.name.substring( 0, item.name.length() - 4 );
                if ( dict.containsKey( key ) )
                {
                    BC_CHUNK_VALUES obj = dict.get( key );
                    obj.files.add( item );
                }
                else
                {
                    BC_CHUNK_VALUES obj = new BC_CHUNK_VALUES( key );
                    obj.files.add( item );
                    dict.put( key, obj );
                }
            }
        }
        for ( BC_CHUNK_VALUES obj : dict.values() )
        {
            BC_CHUNK_VALUES v = new BC_CHUNK_VALUES( obj.name );
            for ( BC_NAME_SIZE ns : obj.files )
            {
                /* get the last 3 characters ( aka the extension ) */
                String ext = ns.name.substring( Math.max( ns.name.length() - 3, 0 ) );
                v.files.add( new BC_NAME_SIZE( ext, ns.size ) );
            }
            res.add( v );
        }
        return res;
    }

}

