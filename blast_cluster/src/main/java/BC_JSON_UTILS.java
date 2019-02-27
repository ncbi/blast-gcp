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

import com.google.gson.Gson;
import com.google.gson.JsonParser;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonArray;
import com.google.gson.stream.JsonReader;

/**
 * utility class to extract values from json using google's parser
 *
*/
class BC_JSON_UTILS
{

/**
 * extract a String-value from a JsonObject, use dflt-value if key not found
 *
 * @param root			the JsonObject to extract the value from
 * @param key			the key of the value
 * @param dflt          the default-value to use if key not found
 * @return 				found value or default-value
*/
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

/**
 * extract a Integer-value from a JsonObject, use dflt-value if key not found
 *
 * @param root			the JsonObject to extract the value from
 * @param key			the key of the value
 * @param dflt          the default-value to use if key not found
 * @return 				found value or default-value
*/
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

/**
 * extract a Long-value from a JsonObject, use dflt-value if key not found
 *
 * @param root			the JsonObject to extract the value from
 * @param key			the key of the value
 * @param dflt          the default-value to use if key not found
 * @return 				found value or default-value
*/
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

/**
 * extract a Double-value from a JsonObject, use dflt-value if key not found
 *
 * @param root			the JsonObject to extract the value from
 * @param key			the key of the value
 * @param dflt          the default-value to use if key not found
 * @return 				found value or default-value
*/
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

/**
 * extract a Boolean-value from a JsonObject, use dflt-value if key not found
 *
 * @param root			the JsonObject to extract the value from
 * @param key			the key of the value
 * @param dflt          the default-value to use if key not found
 * @return 				found value or default-value
*/
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

/**
 * extract a sub-object from a JsonObject
 *
 * @param root			the JsonObject to extract the object from
 * @param key			the key of the object
 * @return 				found object or null
*/
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

/**
 * extract a sub-object from a JsonObject as a string
 *
 * @param root			the JsonObject to extract the object from
 * @param key			the key of the object
 * @return 				string-representation of found object or empty string
*/
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

/**
 * extract an array of JsonObjects from a JsonObject
 *
 * @param root			the JsonObject to extract the array from
 * @param key			the key of the object
 * @return 				the found array or null
*/
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

/**
 * extract a list of strings from a JsonObject
 *
 * @param root			the JsonObject to extract the list from
 * @param key			the key of the object
 * @param dflt		    the single dflt entry if key not found
 * @param lst		    list of strings to be filled with found values
*/
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

}

