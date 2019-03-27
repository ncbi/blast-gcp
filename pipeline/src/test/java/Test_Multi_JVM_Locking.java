package gov.nih.nlm.ncbi.blastjni;

import org.junit.*;
import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.File;
import java.io.IOException;
import java.io.FileReader;
import java.io.BufferedReader;

import java.nio.file.Paths;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;

import java.util.List;
import java.util.ArrayList;
import java.util.Random;

public class Test_Multi_JVM_Locking
{
    /* write a function to create the shell-script */
    static private final String FILE_TO_PROTECT = "testfile.txt";
    static private final String SCRIPT = "run_jvm_locking.sh";

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

    private static boolean make_executable( final String fn )
    {
        File f = new File( fn );
        try
        {
            if ( f.exists() )
            {
                f.setExecutable( true, true );
                return true;
            }
        }
        catch ( Exception e )
        {
            System.out.println( e );
        }
        return false;
    }

    /* count the lines starting with this pattern 'T[?].? = ' */
    private static int count_correct_lines( final String fn, int l )
    {
        int res = 0;
        try ( BufferedReader br = new BufferedReader( new FileReader( fn ) ) )
        {
            for ( String line; ( line = br.readLine() ) != null; )
            {
                if ( line.length() == ( l + 9 ) &&
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

    /* ---------------------------------------------------------------- */
    private class shellThread extends Thread
    {
        private final int id;

        shellThread( int a_id ) { id = a_id; start(); }

        @Override public void run()
        {
            System.out.println( String.format( "start shell-thread #%d", id ) );

            String cwd = System.getProperty( "user.dir" );
            ProcessBuilder pb = new ProcessBuilder( cwd + "/" + SCRIPT );
            try
            {
                Process p = pb.start();
                BufferedReader br = new BufferedReader( new InputStreamReader( p.getInputStream() ) );
                for ( String line; ( line = br.readLine() ) != null; )
                {
                    System.out.println( String.format( "script: %s", line.trim() ) );
                }

                int res = p.waitFor();
                assertTrue( "shell returned code 0", res == 0 );
            }
            catch( Exception e )
            {
                System.out.println( e );
            }

            System.out.println( String.format( "shell-thread #%d done", id ) );
        }
    }

    private int perform_cmd( final String cmd, int numShells, int ll )
    {
        int res = 0;

        delete_file_if_exists( SCRIPT );
        create_file( SCRIPT, String.format( "%s %d\n", cmd, ll ) );
        make_executable( SCRIPT );

        delete_file_if_exists( FILE_TO_PROTECT );
        create_file( FILE_TO_PROTECT, "start\n" );

        List< shellThread > threads = new ArrayList<>();

        for ( int i = 0; i < numShells; ++i ) { threads.add( new shellThread( i ) ); }

        for ( Thread t : threads )
        {
            try { t.join(); }
            catch( InterruptedException e ) { }
        }

        res = count_correct_lines( FILE_TO_PROTECT, ll );
        System.out.println( String.format( "we have %d valid lines", res ) );

        delete_file_if_exists( SCRIPT );
        delete_file_if_exists( FILE_TO_PROTECT );
        return res;
    }


    @Test public void call_a_shell_script()
    {
        int numShells = 20;
        int ll = 25000;
        int n;

        System.out.println( "calling a shell-script with locking: start" );
        n = perform_cmd( "java -cp ./target/classes:./target/test-classes gov.nih.nlm.ncbi.blastjni.Test_BC_FILE_LOCK_V2", numShells, ll );
        assertTrue( "we have to have 200 valid lines", n == ( numShells * 10 ) );
        System.out.println( "calling a shell-script with locking: done\n" );

        System.out.println( "calling a shell-script without locking: start" );
        n = perform_cmd( "java -cp ./target/classes:./target/test-classes gov.nih.nlm.ncbi.blastjni.Test_BC_FILE_LOCK_V2 without", numShells, ll );
        assertTrue( "we may have less than 200 valid lines", n <= ( numShells * 10 ) );
        System.out.println( "calling a shell-script without locking: done\n" );

        System.out.println( "calling a shell-script with stream-lock: start" );
        n = perform_cmd( "java -cp ./target/classes:./target/test-classes gov.nih.nlm.ncbi.blastjni.Test_StreamLock", numShells, ll );
        assertTrue( "we have to have x valid lines", n == ( numShells * 10 ) );
        System.out.println( "calling a shell-script with locking: done\n" );

        System.out.println( "calling a shell-script without stream-lock: start" );
        n = perform_cmd( "java -cp ./target/classes:./target/test-classes gov.nih.nlm.ncbi.blastjni.Test_StreamLock without", numShells, ll );
        assertTrue( "we have to have less than x valid lines", n <= ( numShells * 10 ) );
        System.out.println( "calling a shell-script without locking: done\n" );

    }
}
