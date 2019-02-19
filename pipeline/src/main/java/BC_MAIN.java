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

import java.io.File;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.api.java.JavaRDD;

public final class BC_MAIN
{
	private static Boolean file_exists( String filename )
	{
		File f = new File( filename );
		return f.exists();
	}

	private static void run( final BC_SETTINGS settings )
	{
		/* create and configure the spark-context */
		SparkConf sc = BC_SETTINGS_READER.createSparkConfAndConfigure( settings );
		JavaSparkContext jsc = new JavaSparkContext( sc );
		jsc.setLogLevel( settings.spark_log_level );

		/* broadcast the Debug-settings */
		Broadcast< BC_DEBUG_SETTINGS > DEBUG_SETTINGS = jsc.broadcast( settings.debug );

		HashMap< String, JavaRDD< BC_DATABASE_RDD_ENTRY > > db_dict;

		/* populate db_dict */
		for ( String key : settings.dbs.keySet() )
		{
			BC_DATABASE_SETTING db_setting = settings.dbs.get( key );

			/* get a list of entries from the source ( bucket ) */
			List< String > files = BC_GCP_TOOLS.list( db_setting.source_location );

			/* get a list of unique names ( without the extension ) */
			List< String > names = BC_GCP_TOOLS.unique_without_extension( files, db_setting.extensions );
			System.out.println( String.format( "%s has %d chunks", key, names.size() ) );

			/* create a list of Database-RDD-entries using a static method of this class */
			List< BC_DATABASE_RDD_ENTRY > entries = BC_DATABASE_RDD_ENTRY.make_rdd_entry_list( db_setting, names );

			/* ask the spark-context to distribute the RDD to the workers */
			JavaRDD< BC_DATABASE_RDD_ENTRY > rdd = sc.parallelize( entries );

			/* put the RDD in the database-dictionary */
			db_dict.put( key, rdd );
		}

		/* for each RDD in the database-dictionary run a simple map-reduce operation */
		for ( String key : db_dict.keySet() )
		{
			JavaRDD< BC_DATABASE_RDD_ENTRY rdd = db_dict.get( key );

		}
	}

    public static void main( String[] args ) throws Exception
    {
        if ( args.length < 1 )
            System.out.println( "settings-file not specified" );
        else
        {
			String settings_file_name = args[ 0 ];
			if ( file_exists( settings_file_name ) )
			{
				System.out.println( String.format( "reading settings from file: '%s'", settings_file_name ) );
				/* parse the settings-file */
				BC_SETTINGS settings = BC_SETTINGS_READER.read_from_json( settings_file_name, BC_MAIN.class.getSimpleName() );
				System.out.println( settings );
				if ( !settings.valid() )
					System.out.println( "settings are invalid!, exiting..." );
				else
				{
					System.out.println( "settings are valid! preparing cluster" );

					/* === run the application === */
					run( settings );
				}
			}
			else
				System.out.println( String.format( "settings-file '%s' not found", settings_file_name ) );
        }
   }
}

