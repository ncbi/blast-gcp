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

package gov.nih.nlm.ncbi.blast_spark_cluster;

import java.util.List;
import java.util.ArrayList;
import java.net.UnknownHostException;

import com.google.gson.Gson;
import com.google.gson.JsonParser;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonArray;
import com.google.gson.stream.JsonReader;

class BC_JSON_UTILS
{
    public static String get_json_string( JsonObject root, final String key, final String dflt )
    {
        String res = dflt;
        if ( root != null )
        {
            JsonElement elem = root.get( key );
            if ( elem != null )
            {
                try
                {
                    res = elem.getAsString();
                }
                catch( Exception e )
                {
                    System.out.println( String.format( "json-parsing for %s -> %s", key, e ) );
                }
            }
        }
        return res;
    }

    public static Integer get_json_int( JsonObject root, final String key, final Integer dflt )
    {
        Integer res = dflt;
        if ( root != null )
        {
            JsonElement elem = root.get( key );
            if ( elem != null )
            {
                try
                {
                    res = elem.getAsInt();
                }
                catch( Exception e )
                {
                    System.out.println( String.format( "json-parsing for %s -> %s", key, e ) );
                }
            }
        }
        return res;
    }

    public static Long get_json_long( JsonObject root, final String key, final Long dflt )
    {
        Long res = dflt;
        if ( root != null )
        {
            JsonElement elem = root.get( key );
            if ( elem != null )
            {
                try
                {
                    res = elem.getAsLong();
                }
                catch( Exception e )
                {
                    System.out.println( String.format( "json-parsing for %s -> %s", key, e ) );
                }
            }
        }
        return res;
    }

    public static Double get_json_double( JsonObject root, final String key, final Double dflt )
    {
        Double res = dflt;
        if ( root != null )
        {
            JsonElement elem = root.get( key );
            if ( elem != null )
            {
                try
                {
                    res = elem.getAsDouble();
                }
                catch( Exception e )
                {
                    System.out.println( String.format( "json-parsing for %s -> %s", key, e ) );
                }
            }
        }
        return res;
    }

    public static Boolean get_json_bool( JsonObject root, final String key, final Boolean dflt )
    {
        Boolean res = dflt;
        if ( root != null )
        {
            JsonElement elem = root.get( key );
            if ( elem != null )
            {
                try
                {
                    res = elem.getAsBoolean();
                }
                catch( Exception e )
                {
                    System.out.println( String.format( "json-parsing for %s -> %s", key, e ) );
                }
            }
        }
        return res;
    }

    public static JsonObject get_sub( JsonObject root, final String key )
    {
        if ( root != null )
        {
            JsonElement e = root.get( key );
            if ( e != null )
            {
                if ( e.isJsonObject() )
                    return e.getAsJsonObject();
            }
        }
        return null;
    }

    public static String get_sub_as_string( JsonObject root, final String key )
    {
        String res = "";
        if ( root != null )
        {
            JsonElement e = root.get( key );
            if ( e != null )
                res = e.toString();
        }
        return res;
    }

    public static JsonArray get_sub_array( JsonObject root, final String key )
    {
        if ( root != null )
        {
            JsonElement e = root.get( key );
            if ( e != null )
            {
                if ( e.isJsonArray() )
                    return e.getAsJsonArray();
            }
        }
        return null;
    }

	public static String get_local_host( final String dflt )
	{
        String res = dflt;
        try
        {
            java.net.InetAddress localMachine = java.net.InetAddress.getLocalHost();
            res = localMachine.getHostName();
        }
        catch ( UnknownHostException e )
        {
            res = dflt;
        }
		return res;
	}

    public static String get_host( JsonObject root, final String key, final String dflt )
    {
        return get_json_string( root, key, get_local_host( dflt ) );
    }

    public static String insert_username( final String pattern )
    {
        final String username = System.getProperty( "user.name" );
        return String.format( pattern, username );
    }

    public static void get_string_list( JsonObject root, final String key, final String dflt, List< String > lst )
    {
        try
        {
            if ( root != null )
            {
                JsonArray a = root.getAsJsonArray( key );
                Gson googleJson = new Gson();
                ArrayList l = googleJson.fromJson( a, ArrayList.class );
                if ( l.isEmpty() )
                {
                    if ( dflt != null )
                        lst.add( dflt );
                }
                else
                {
                    for ( int i = 0; i < l.size(); ++i )
                        lst.add( l.get( i ).toString() );
                }
            }
            else if ( dflt != null )
                lst.add( dflt );
        }
        catch ( Exception e )
        {
            if ( dflt != null )
                lst.add( dflt );
        }
    }

	public static byte[] hexStringToByteArray( String s )
	{
    	int len = s.length();
    	byte[] data = new byte[ len / 2 ];
    	for ( int i = 0; i < len; i += 2 )
		{
			int c0 = Character.digit( s.charAt( i ), 16 );
			int c1 = Character.digit( s.charAt( i + 1 ), 16 );
        	data[ i / 2 ] = (byte) ( ( c0 << 4 ) + c1 );
    	}
    	return data;
	}

}

