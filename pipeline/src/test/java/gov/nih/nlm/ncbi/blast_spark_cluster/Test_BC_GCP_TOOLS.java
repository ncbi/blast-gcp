package gov.nih.nlm.ncbi.blastjni;

import org.junit.*;
import static org.junit.Assert.*;

import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.io.File;
import java.io.InputStream;



public class Test_BC_GCP_TOOLS {

    static public final String GS_BUCKET_DFLT = "yan-blastdb";
    static public final String GS_KEY_DFLT = "test";
    static public final String GS_URI_DFLT = "gs://blast-db/latest-dir";
    static public final String DST_FILE_DFLT = "/tmp/gs_file.out";

    static private BC_GCP_TOOLS tools = null;

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
    public void setupTools() {
        tools = BC_GCP_TOOLS.getInstance();
        assertNotNull(tools);
    }

    @Test
    public void testDownloadToFile() {
        Boolean ok = tools.download(gs_uri, dst_file);
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
        InputStream is = tools.download(gs_uri);
        if (is == null) {
            fail("NULL InputStream");
        }
    }

    @Test
    public void testUpload() {
        String testStr = "This is a test";
        int length = testStr.length();
        Integer check_cnt = BC_GCP_TOOLS.upload(gs_bucket, gs_key, "This is a test");
        assertTrue(length == check_cnt.intValue());
    }

    @Test
    public void testList() {
        List< String > listing = BC_GCP_TOOLS.list("gs://" + gs_bucket);
        assertTrue(!listing.isEmpty());
        int ii = 0;
        for (String entry : listing) {
            System.out.println(entry);
            if (++ii == 10) break;
        }
    }

    @Test
    public void testUniqueWithoutExt() {
        List< String > all = new ArrayList();
        all.add("anucl1.ndb");
        all.add("anucl1.nhr");
        all.add("anucl1.nin");
        all.add("anucl1.nsq");
        all.add("aprot1.pdb");
        all.add("aprot1.psq");

        List< String > ext = new ArrayList();
        ext.add("ndb");
        ext.add("nhr");
        ext.add("nin");
        ext.add("nsq");
        ext.add("pdb");
        ext.add("psq");

        List< String > res = BC_GCP_TOOLS.unique_without_extension(all, ext);
        assertTrue(res.size() == 2);
    }
}
