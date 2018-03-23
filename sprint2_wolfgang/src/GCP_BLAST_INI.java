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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GCP_BLAST_INI
{

   private Pattern _section  = Pattern.compile( "\\s*\\[([^]]*)\\]\\s*" );
   private Pattern _keyValue = Pattern.compile( "\\s*([^=]*)=(.*)" );
   private Map< String, Map< String, String > > _entries  = new HashMap<>();

   public GCP_BLAST_INI( final String path ) throws IOException
   {
      load( path );
   }

   public void load( final String path ) throws IOException
   {
      try( BufferedReader br = new BufferedReader( new FileReader( path ) ) )
      {
         String line;
         String section = null;
         while ( ( line = br.readLine() ) != null )
         {
            Matcher m = _section.matcher( line );
            if ( m.matches() )
            {
               section = m.group( 1 ).trim();
            }
            else if ( section != null )
            {
               m = _keyValue.matcher( line );
               if ( m.matches() )
               {
                  String key   = m.group( 1 ).trim();
                  String value = m.group( 2 ).trim();
                  Map< String, String > kv = _entries.get( section );
                  if ( kv == null )
                    _entries.put( section, kv = new HashMap<>() );
                  kv.put( key, value );
               }
            }
         }
      }
   }

    public String getString( final String section, final String key, final String defaultvalue )
    {
        String res = defaultvalue;
        Map< String, String > kv = _entries.get( section );
        if ( kv != null )
        {
            String value = kv.get( key );
            if ( value != null )
                res = value;
        }
        return res;
   }

    public int getInt( final String section, final String key, int defaultvalue )
    {
        int res = defaultvalue;
        Map< String, String > kv = _entries.get( section );
        if ( kv != null )
        {
            String value = kv.get( key );
            if ( value != null )
            {
                try
                {
                    res = Integer.parseInt( value );
                }
                catch ( Exception e )
                {
                    res = defaultvalue;
                }
            }
        }
        return res;
   }

    public float getFloat( final String section, final String key, float defaultvalue )
    {
        float res = defaultvalue;
        Map< String, String > kv = _entries.get( section );
        if ( kv != null )
        {
            String value = kv.get( key );
            if ( value != null )
            {
                try
                {
                    res = Float.parseFloat( value );
                }
                catch ( Exception e )
                {
                    res = defaultvalue;
                }
            }
        }
        return res;
    }

    public double getDouble( final String section, final String key, double defaultvalue )
    {
        double res = defaultvalue;
        Map< String, String > kv = _entries.get( section );
        if ( kv != null )
        {
            String value = kv.get( key );
            if ( value != null )
            {
                try
                {
                    res = Double.parseDouble( value );
                }
                catch ( Exception e )
                {
                    res = defaultvalue;
                }
            }
        }
        return res;
   }
   
    public Boolean getBoolean( final String section, final String key, Boolean defaultvalue )
    {
        Boolean res = defaultvalue;
        Map< String, String > kv = _entries.get( section );
        if ( kv != null )
        {
            String value = kv.get( key );
            if ( value != null )
            {
                try
                {
                    res = Boolean.parseBoolean( value );
                }
                catch ( Exception e )
                {
                    res = defaultvalue;
                }
            }
        }
        return res;
   }
}
