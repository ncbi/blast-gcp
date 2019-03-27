package gov.nih.nlm.ncbi.blastjni;

import org.junit.*;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;

import java.nio.file.Paths;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;

import java.util.List;
import java.util.ArrayList;
import java.util.Random;

public class Test_BC_FILE_LOCK
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

    private static boolean create_file( final String fn, final String txt )
    {
        boolean res = false;
        try
        {
            Files.write( Paths.get( fn ), txt.getBytes(), StandardOpenOption.CREATE );
            res = true;
        }
        catch ( IOException e )
        {
            System.out.println( e );
        }
        return res;
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

    /* count the lines starting with this pattern 'T[?].? = ' */
    private static int count_correct_lines( final String fn )
    {
        int res = 0;
        try ( BufferedReader br = new BufferedReader( new FileReader( fn ) ) )
        {
            for ( String line; ( line = br.readLine() ) != null; )
            {
                if ( line.length() == 10009 &&
                     line.charAt( 0 ) == 'T' &&
                     line.charAt( 1 ) == '[' &&
                     line.charAt( 3 ) == ']' &&
                     line.charAt( 4 ) == '.' &&
                     line.charAt( 6 ) == ' ' &&
                     line.charAt( 7 ) == '=' &&
                     line.charAt( 8 ) == ' ' )
                {
                    res++;
                }
            }
        }
        catch( Exception e )
        {
            System.out.println( e );
        }
        return res;
    }

    private class lockThread extends Thread
    {
        private final int id;

        lockThread( int a_id ) { id = a_id; start(); }

        @Override public void run()
        {
            int i = 0;
            System.out.println( String.format( "start thread #%d", id ) );
            while ( i < 10 )
            {
                try ( BC_FILE_LOCK lock = new BC_FILE_LOCK( FILE_TO_PROTECT ) )
                {
                    if ( lock.locked() )
                    {
                        assertTrue( "lock-dir should exist now", lock_dir_exists( FILE_TO_PROTECT ) );
                        boolean ok = append_to_file( FILE_TO_PROTECT, String.format( "T[%d].%d = %s\n", id, i, randomString( 10000 ) ) );
                        assertTrue( "write to file should not fail", ok );
                        if ( ok ) i++;
                    }
                    else
                    {
                        sleep_for( rand_between( 100, 200 ) );
                    }
                }
            }
            System.out.println( String.format( "thread #%d done", id ) );
        }
    }

    private class noLockThread extends Thread
    {
        private final int id;

        noLockThread( int a_id ) { id = a_id; start(); }

        @Override public void run()
        {
            int i = 0;
            System.out.println( String.format( "start thread #%d", id ) );
            while ( i < 10 )
            {
                boolean ok = append_to_file( FILE_TO_PROTECT, String.format( "T[%d].%d = %s\n", id, i, randomString( 10000 ) ) );
                assertTrue( "write to file should not fail", ok );
                if ( ok ) i++;
            }
            System.out.println( String.format( "thread #%d done", id ) );
        }
    }

    @Test public void test_single_thread_locking()
    {
        System.out.println( "test single-thread locking() : start" );
        assertFalse( "lock-dir should not exist here", lock_dir_exists( FILE_TO_PROTECT ) );
        try ( BC_FILE_LOCK lock1 = new BC_FILE_LOCK( FILE_TO_PROTECT ) )
        {
            assertTrue( "lock1 should be aquired now", lock1.locked() );
            assertTrue( "lock-dir should exist now", lock_dir_exists( FILE_TO_PROTECT ) );
            try ( BC_FILE_LOCK lock2 = new BC_FILE_LOCK( FILE_TO_PROTECT ) )
            {
                assertFalse( "lock2 should not be aquired here", lock2.locked() );
                assertTrue( "lock-dir should still exist", lock_dir_exists( FILE_TO_PROTECT ) );
            }
            assertTrue( "lock-dir should still exist #2", lock_dir_exists( FILE_TO_PROTECT ) );
        }
        assertFalse( "lock-dir should be gone now", lock_dir_exists( FILE_TO_PROTECT ) );
        System.out.println( "test single-thread locking() : OK" );
    }

    @Test public void test_multi_thread_locking()
    {
        System.out.println( "test multi-thread locking() : start" );

        delete_file_if_exists( FILE_TO_PROTECT );
        assertTrue( "should be able to create file", create_file( FILE_TO_PROTECT, "start\n" ) );

        List< lockThread > threads = new ArrayList<>();

        assertFalse( "lock-dir should not exist", lock_dir_exists( FILE_TO_PROTECT ) );
        for ( int i = 0; i < 10; ++i ) { threads.add( new lockThread( i ) ); }

        for ( Thread t : threads )
        {
            try { t.join(); }
            catch( InterruptedException e ) { }
        }
        assertFalse( "lock-dir should not exist", lock_dir_exists( FILE_TO_PROTECT ) );

        int n = count_correct_lines( FILE_TO_PROTECT );
        System.out.println( String.format( "we have %d valid lines", n ) );
        assertTrue( "we should have 100 valid lines", ( n == 100 ) );
        delete_file_if_exists( FILE_TO_PROTECT );

        System.out.println( "test multi-thread locking() : OK" );
    }

    @Test public void test_multi_thread_no_locking()
    {
        System.out.println( "test multi-thread no-locking() : start" );

        delete_file_if_exists( FILE_TO_PROTECT );
        assertTrue( "should be able to create file", create_file( FILE_TO_PROTECT, "start\n" ) );

        List< noLockThread > threads = new ArrayList<>();

        for ( int i = 0; i < 10; ++i ) { threads.add( new noLockThread( i ) ); }

        for ( Thread t : threads )
        {
            try { t.join(); }
            catch( InterruptedException e ) { }
        }

        int n = count_correct_lines( FILE_TO_PROTECT );
        System.out.println( String.format( "we have %d valid lines", n ) );
        assertTrue( "we should have less than, or equal to 100 valid lines", ( n <= 100 ) );
        delete_file_if_exists( FILE_TO_PROTECT );

        System.out.println( "test multi-thread no-locking() : OK" );
    }

}

