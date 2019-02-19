package gov.nih.nlm.ncbi.blastjni;

import org.junit.*;
import static org.junit.Assert.*;

import java.util.Map;
import java.io.File;
import java.io.InputStream;

import com.google.api.services.storage.Storage;


public class Test_BLAST_GS_DOWNLOADER {

    static public final String GS_BUCKET_DFLT = "blast-db";
    static public final String GS_KEY_DFLT = "latest-dir";
    static public final String GS_URI_DFLT = "gs://blast-db/latest-dir";
    static public final String DST_FILE_DFLT = "/tmp/gs_file.out";

    static private Storage storage = null;

    static private String gs_bucket;
    static private String gs_key;
    static private String gs_uri;
    static private String dst_file;

    @BeforeClass
    public static void setUpEnv() {
        Map<String, String> env = System.getenv();
        gs_bucket = (String) env.getOrDefault("GS_BUCKET", GS_BUCKET_DFLT);
        gs_key = (String) env.getOrDefault("GS_KEY", GS_KEY_DFLT);
        gs_uri = (String) env.getOrDefault("GS_KEY", GS_URI_DFLT);
        dst_file = (String) env.getOrDefault("DST_FILE", DST_FILE_DFLT);
    }

    @Before
    public void setupStorage() {
        try {
            storage = BLAST_GS_DOWNLOADER.buildStorageService();
            assertNotNull(storage);
        }
        catch (java.security.GeneralSecurityException ex) {
            ex.printStackTrace();
            fail();
        }
        catch (java.io.IOException ex) {
            ex.printStackTrace();
            fail();
        }
    }

    @Test
    public void testDownload() {
        Boolean ok = BLAST_GS_DOWNLOADER.download(storage, gs_bucket, gs_key, dst_file);
        if (!ok) {
            fail();
        }
        // check file is present
        File ff = new File(dst_file);
        if (!ff.exists() || ff.length() == 0) {
            fail("destination file " + dst_file + " not found or has zero length");
        }
    }

    @Test
    public void testDownloadAsStream() {
        InputStream is = BLAST_GS_DOWNLOADER.download_as_stream(storage, gs_bucket, gs_key);
        if (is == null) {
            fail("NULL InputStream");
        }
    }

    @Test
    public void testDownloadUri() {
        Boolean ok = BLAST_GS_DOWNLOADER.download_uri(storage, gs_uri, dst_file);
        if (!ok) {
            fail();
        }
        // check file is present
        File ff = new File(dst_file);
        if (!ff.exists() || ff.length() == 0) {
            fail("destination file " + dst_file + " not found or has zero length");
        }
    }

    @Test
    public void testDownloadUriAsStream() {
        InputStream is = BLAST_GS_DOWNLOADER.download_uri_as_stream(storage, gs_uri);
        if (is == null) {
            fail("NULL InputStream");
        }
    }
}
