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

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * utility class to process a list of requests
 * common ancestor for BC_FILE_LIST and BC_BUCKET_LIST
 * - stores application context
 * - stores name of the source of the list ( file or bucket )
 * - stores optional limit, 0..no limit
 * - stores a PrintStream to report back to the stream where the request for the list came from
 * - owns a line-number to apply the limit if needed
 * - owns a running-flag to stop the list on request
 *
 * @see BC_CONTEXT
*/
class BC_LIST extends Thread
{
    protected final BC_CONTEXT context;
    protected final String srcName;
    protected final int limit;
    protected int line_nr = 0;
    protected final AtomicBoolean running;
    protected final Logger logger;

/**
 * create instance of BC_LIST
 *
 * @param a_context    application-context
 * @param a_srcName    list-file or bucket-name
 * @param a_ps         PrintStream to write debug-output
 * @param a_limit      limit number of jobs in the list
 * @see        BC_CONTEXT
*/
    public BC_LIST( final BC_CONTEXT a_context, final String a_srcName, int a_limit )
    {
        context = a_context;
        srcName = a_srcName;
        limit = a_limit;
        line_nr = 0;
        running = new AtomicBoolean( true );
        logger = LogManager.getLogger( BC_LIST.class );
    }

/**
 * check if we are still running:
 * - application did not terminate
 * - list has not been asked to stop
 * - if there is a limit, are we still under the limit?
 * > is used by derived classes
 *
 * @return is the list still running ?
*/
    protected boolean is_running()
    {
        boolean res = ( context.is_running() && running.get() );
        if ( res && limit > 0 ) res = ( line_nr < limit );
        return res;
    }

/**
 * helper-method to loop until the application-context has accepted
 * the given request-file ( it can reject if the queue is full )
 * > is used by derived classes
 *
 * @param request_filename name/url of request-file to be queued in app.-context
*/

    protected void submitFile( final String request_filename )
    {
        boolean done = false;
        while( context.is_running() && !done )
        {
            int res = context.add_request_file( request_filename );
            done = ( res != 0 );    /* 0...not done( because no space in queue, 1...done, -1...invalid */
            if ( context.is_running() && !done )
            {
                try
                {
                    Thread.sleep( 250 );
                }
                catch ( InterruptedException e ) { }
            }
        }
        line_nr += 1;
    }
}

/**
 * utility class to process a list of requests from a file on the local filesystem
 * derived from BC_LIST
 *
 * @see BC_CONTEXT
 * @see BC_LIST
*/
class BC_FILE_LIST extends BC_LIST
{

/**
 * create instance of BC_FILE_LIST
 *
 * @param a_context    application-context
 * @param a_srcName    list-file
 * @param a_ps         PrintStream to write debug-output
 * @param a_limit      limit number of jobs in the list
 * @see        BC_CONTEXT
*/
    public BC_FILE_LIST( final BC_CONTEXT a_context, final String a_srcName, int a_limit )
    {
        super( a_context, a_srcName, a_limit );
    }

/**
 * overwritten run method of Thread-BC_FILE_LIST
 * - create a buffered-reader for the input-file
 * - loop until application closed, no more lines, limit reached
 * - read line, skip lines starting with '#',
 *   detect lines starting with ':src=' and store src-bucket,
 *   submit line by calling BC_LIST.submitFile()
 *
*/
    @Override public void run()
    {
        logger.info( String.format( "request-list '%s' start", srcName ) );
        try
        {
            FileInputStream fs = new FileInputStream( srcName );
            BufferedReader br = new BufferedReader( new InputStreamReader( fs ) );
            String line;
            String src = "";

            while ( is_running() && ( ( line = br.readLine() ) != null ) )
            {
                String tline = line.trim();
                if ( !tline.isEmpty() && !tline.startsWith( "#" ) )
                {
                    if ( tline.startsWith( ":src=" ) )
                    {
                        try { src = tline.substring( 5 ).trim(); }
                        catch( Exception e ) {}
                    }
                    else if ( tline.startsWith( ":wait" ) )
                    {
                        int time_limit = 0;
                        try { time_limit = BC_UTILS.toInt( tline.substring( 6 ).trim() ); }
                        catch( Exception e ) {}
                        context.wait_for_empty( time_limit );
                    }
                    else if ( tline.startsWith( ":exit" ) )
                    {
                        context.stop();
                    }
                    else if ( tline.startsWith( ":pick" ) )
                    {
                        int limit = 0;
                        try { limit = BC_UTILS.toInt( tline.substring( 5 ).trim() ); }
                        catch( Exception e ) {}
                        if ( !src.isEmpty() )
                        {
                            context.addRequestBucket( src, limit );
                            try { Thread.sleep( 2000 ); }
                            catch ( InterruptedException e ) { }
                        }
                    }
                    else
                    {
                        if ( !src.isEmpty() )
                            submitFile( String.format( "%s/%s", src, tline ) );
                        else
                            submitFile( tline );
                    }
                }
            }
            br.close();
        }
        catch( Exception e )
        {
            logger.info( String.format( "request-list '%s' : %s", srcName, e ) );
        }
        logger.info( String.format( "request-list '%s' done", srcName ) );
    }
}

/**
 * utility class to process a list of requests from a bucket
 * derived from BC_LIST
 *
 * @see BC_CONTEXT
 * @see BC_LIST
*/
class BC_BUCKET_LIST extends BC_LIST
{
    private List< String > files;

/**
 * create instance of BC_FILE_LIST
 *
 * @param a_context    application-context
 * @param a_srcName    bucket-url
 * @param a_ps         PrintStream to write debug-output
 * @param a_limit      limit number of jobs in the list
 * @see        BC_CONTEXT
*/
    public BC_BUCKET_LIST( final BC_CONTEXT a_context, final String a_srcName, int a_limit )
    {
        super( a_context, a_srcName, a_limit );
        logger.info( String.format( "bucket-list '%s' reading files... ( limit %d )", srcName, limit ) );
        files = BC_GCP_TOOLS.list_names( srcName, limit, "json" );
    }

/**
 * overwritten run method of Thread-BC_FILE_LIST
 * - call BC_GCP_TOOLS.list() to get a list of all entries in bucket
 * - loop until application closed, no more lines, limit reached
 * - get entry-name,
 *   submit line by calling BC_LIST.submitFile()
 *
*/
    @Override public void run()
    {
        logger.info( String.format( "bucket-list '%s' start", srcName ) );

        Iterator< String > iter = files.iterator();

        while ( is_running() && iter.hasNext() )
        {
            submitFile( String.format( "%s/%s", srcName, iter.next() ) );
        }

        logger.info( String.format( "bucket-list '%s' done", srcName ) );
    }
}

/**
 * utility class to manage a list of BC_LIST instances
 *
 * @see BC_CONTEXT
 * @see BC_LIST
*/
public final class BC_LISTS
{
    private final BC_CONTEXT context;
    private final List< BC_LIST > lists;

/**
 * create instance of BC_LIST
 *
 * @param a_context    application-context
 * @see        BC_CONTEXT
*/
    public BC_LISTS( final BC_CONTEXT a_context )
    {
        context = a_context;
        lists = new ArrayList<>();
    }

/**
 * helper function to join one BC_LIST instance
 *
 * @param     list BC_LIST instance to join
 * @see       BC_LIST
*/
    private void join_list( BC_LIST list )
    {
        try { list.join(); }
        catch( InterruptedException e ) { }
    }

/**
 * helper function to join all BC_LIST-instances that are terminated
 *
 * @see       BC_LIST
*/
    private void join_done_list()
    {
        for ( BC_LIST list : lists )
        {
            if ( list.getState() == Thread.State.TERMINATED )
                join_list( list );
        }
    }

/**
 * helper function to add a new BC_FILE_LIST
 *
 * @param     filename   filename of list to process
 * @param     limit      limit number of jobs in the list
 * @see       BC_LIST
*/
    public void addFile( final String filename, int limit )
    {
        /* try to join lists that are done */
        join_done_list();

        /* create a new list, and start it */
        BC_LIST list = new BC_FILE_LIST( context, filename, limit );
        lists.add( list );
        list.start();
    }

/**
 * helper function to add a new BC_BUCKET_LIST
 *
 * @param     bucketName url of bucket to process
 * @param     limit      limit number of jobs in the list
 * @see       BC_LIST
*/
    public void addBucket( final String bucketName, int limit )
    {
        /* try to join lists that are done */
        join_done_list();

        /* create a new list, and start it */
        BC_LIST list = new BC_BUCKET_LIST( context, bucketName, limit );
        lists.add( list );
        list.start();
    }

/**
 * helper function to join all running and terminated threads
 *
*/
    public void join()
    {
        for ( BC_LIST list : lists )
            join_list( list );
    }

/**
 * helper function to stop all running list threads
 *
*/
    public void stop()
    {
        for ( BC_LIST list : lists )
            list.running.set( false );
    }
}

