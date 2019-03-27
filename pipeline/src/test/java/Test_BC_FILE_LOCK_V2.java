package gov.nih.nlm.ncbi.blastjni;

import org.junit.*;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;

import java.nio.file.Paths;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;

import java.util.List;
import java.util.ArrayList;
import java.util.Random;

public class Test_BC_FILE_LOCK_V2
{
    static private final String FILE_TO_PROTECT = "testfile.txt";

    private static boolean lock_dir_exists( final String to_protect )
    {
        File f = new File( String.format( "%s.lock", to_protect ) );
        return f.exists();
    }

    private static void delete_file_if_exists( final String fn )
    {
        File f = new File( fn );
        try
        {
            if ( f.exists() ) f.delete();
        }
        catch ( Exception e )
        {
            System.out.println( e );
        }
    }

    private static boolean append_to_file( final String fn, final String txt )
    {
        boolean res = false;
        try
        {
            Files.write( Paths.get( fn ), txt.getBytes(), StandardOpenOption.APPEND );
            res = true;
        }
        catch ( IOException e )
        {
            System.out.println( e );
        }
        return res;
    }

    private static void sleep_for( int ms )
    {
        try { Thread.sleep( ms ); }
        catch ( InterruptedException e ) { }
    }

    private static int rand_between( int low, int high )
    {
        Random rand = new Random( System.currentTimeMillis() );
        return rand.nextInt( high - low ) + low;
    }

    static private final String characters = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    static private final Random rand = new Random( System.currentTimeMillis() );

    private static String randomString( int len )
    {
        StringBuilder sb = new StringBuilder( len );
        for( int i = 0; i < len; i++ )
            sb.append( characters.charAt( rand.nextInt( characters.length() ) ) );
        return sb.toString();
    }

    private class lockThread extends Thread
    {
        private final int id;
        private final int ll;
        private boolean with_lock;

        lockThread( int a_id, boolean a_with_lock, int a_ll )
        {
            id = a_id;
            with_lock = a_with_lock;
            ll = a_ll;
            start();
        }

        private void run_with_lock()
        {
            int i = 0;
            while ( i < 10 )
            {
                try ( BC_FILE_LOCK lock = new BC_FILE_LOCK( FILE_TO_PROTECT ) )
                {
                    if ( lock.locked() )
                    {
                        boolean ok = append_to_file( FILE_TO_PROTECT, String.format( "T[%d].%d = %s\n", id, i, randomString( ll ) ) );
                        if ( ok ) i++;
                    }
                    else
                        sleep_for( rand_between( 100, 200 ) );
                }
            }
        }

        private void run_without_lock()
        {
            int i = 0;
            while ( i < 10 )
            {
                boolean ok = append_to_file( FILE_TO_PROTECT, String.format( "T[%d].%d = %s\n", id, i, randomString( ll ) ) );
                if ( ok ) i++;
                sleep_for( rand_between( 100, 200 ) );
            }
        }

        @Override public void run()
        {
            if ( with_lock )
                run_with_lock();
            else
                run_without_lock();
        }
    }

    public void test_multi_thread_locking( boolean with_lock, int ll )
    {
        List< lockThread > threads = new ArrayList<>();
        for ( int i = 0; i < 1; ++i ) { threads.add( new lockThread( i, with_lock, ll ) ); }

        for ( Thread t : threads )
        {
            try { t.join(); }
            catch( InterruptedException e ) { }
        }
    }

    private static int toInt( String s )
    {
        int res = 0;
        try { res = Integer.parseInt( s ); }
        catch ( NumberFormatException e ) { res = 0; }
        return res;
    }

    public static void main( String[] args ) throws Exception
    {
        boolean with_lock = true;
        int ll = 10000;
        for ( int i = 0; i < args.length; i++ )
        {
            String s = args[ i ];
            if ( s.equals( "without" ) )
                with_lock = false;
            else
            {
                int value = toInt( s );
                if ( value > 0 )
                    ll = value;
            }
        }
        Test_BC_FILE_LOCK_V2 obj = new Test_BC_FILE_LOCK_V2();
        obj.test_multi_thread_locking( with_lock, ll );
    }
}

