package gov.nih.nlm.ncbi.blastjni;

import static org.junit.Assert.*;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

public class Test_BC_DATABASE_RDD_ENTRY extends Thread {

  public static final String CFG_FILE_DFLT = "ini.json";

  private static String cfg_file;

  @BeforeClass
  public static void setUpEnv() {
    final Map<String, String> env = System.getenv();
    cfg_file = (String) env.getOrDefault("CFG_FILE", CFG_FILE_DFLT);
  }

  @Test
  public void test_make_rdd_entry() {
    System.out.println("Using config file: " + cfg_file);
    final BC_SETTINGS res = BC_SETTINGS_READER.read_from_json(cfg_file, "junit");

    assertTrue("dbs must not be empty", !res.dbs.isEmpty());
    for (final String key : res.dbs.keySet()) {
      BC_DATABASE_SETTING setting = res.dbs.get(key);

      List<BC_CHUNK_VALUES> chunks = new ArrayList<>();
      BC_CHUNK_VALUES chunk = new BC_CHUNK_VALUES(key);
      chunks.add(chunk);

      final List<BC_DATABASE_RDD_ENTRY> entryList =
          BC_DATABASE_RDD_ENTRY.make_rdd_entry_list(setting, chunks);
      assertTrue("entryList must not be empty", !entryList.isEmpty());
      for (final BC_DATABASE_RDD_ENTRY entry : entryList) {
        assertTrue("list entry is not NULL", entry != null);
        assertTrue("Name is not empty", !entry.chunk.name.isEmpty());
      }
    }
  }

  @Rule public TemporaryFolder tempfolder = new TemporaryFolder();

  @Test
  public void test_dbload() throws Exception {
    final Random rng = new Random();
    System.out.println("Using config file: " + cfg_file);
    final BC_SETTINGS res = BC_SETTINGS_READER.read_from_json(cfg_file, "junit");
    System.out.println("res is " + res);
    assertTrue("dbs must not be empty", !res.dbs.isEmpty());

    final String key = "nr";
    final BC_DATABASE_SETTING setting = res.dbs.get(key);
    try {
      final File tmpfol = tempfolder.newFolder("db");
      setting.worker_location = tmpfol.getAbsolutePath();
    } catch (IllegalStateException is) {
      setting.worker_location = "/tmp/blast/dbstress";
    } // If running outside junit

    final List<BC_NAME_SIZE> files = BC_GCP_TOOLS.list(setting.source_location);

    final List<BC_CHUNK_VALUES> allChunks =
        BC_GCP_TOOLS.unique_by_extension(files, setting.extensions);

    final List<BC_DATABASE_RDD_ENTRY> entries =
        BC_DATABASE_RDD_ENTRY.make_rdd_entry_list(setting, allChunks);
    final List<String> errorList = new ArrayList<>();
    final List<String> infoList = new ArrayList<>();
    boolean errors;
    int part = rng.nextInt(entries.size());
    part = 3;
    final BC_DATABASE_RDD_ENTRY entry = entries.get(part);
    assertNotNull(entry);
    /*
    System.out.println("name is " + entry.chunk.name);
    System.out.println("files is " + entry.chunk.files);
    System.out.println("worker is " + entry.workername());
    System.out.println("present " + entry.present());
    */
    for (final BC_NAME_SIZE obj : entry.chunk.files) {
      final String dst = entry.build_worker_path(obj.name);
      System.out.println("  dst is:" + dst);
      final File dbfile = new File(dst);
      assertFalse(dbfile.exists());
    }
    assertFalse(entry.present());
    errors = entry.download(errorList, infoList);
    // assertFalse(errors);
    assertTrue(entry.present());
    for (final BC_NAME_SIZE obj : entry.chunk.files) {
      final String dst = entry.build_worker_path(obj.name);
      final File dbfile = new File(dst);
      assertTrue(dbfile.exists());
      assertEquals(dbfile.length(), obj.size.longValue());
    }
  }

  private void stress_dbload() {
    final Random rng = new Random();
    System.out.println("Using config file: " + cfg_file);
    final BC_SETTINGS res = BC_SETTINGS_READER.read_from_json(cfg_file, "junit");
    System.out.println("res is " + res);
    assertTrue("dbs must not be empty", !res.dbs.isEmpty());

    final String key = "nr";
    final BC_DATABASE_SETTING setting = res.dbs.get(key);
    setting.worker_location = "/tmp/blast/dbstress";

    final List<BC_NAME_SIZE> files = BC_GCP_TOOLS.list(setting.source_location);

    final List<BC_CHUNK_VALUES> allChunks =
        BC_GCP_TOOLS.unique_by_extension(files, setting.extensions);

    final List<BC_DATABASE_RDD_ENTRY> entries =
        BC_DATABASE_RDD_ENTRY.make_rdd_entry_list(setting, allChunks);
    final List<String> errorList = new ArrayList<>();
    final List<String> infoList = new ArrayList<>();
    boolean errors;
    int part = rng.nextInt(entries.size());
    part = 3;
    final BC_DATABASE_RDD_ENTRY entry = entries.get(part);
    assertNotNull(entry);
    /*
    System.out.println("name is " + entry.chunk.name);
    System.out.println("files is " + entry.chunk.files);
    System.out.println("worker is " + entry.workername());
    System.out.println("present " + entry.present());
    */
    for (int i = 0; i != 10000; ++i) {
      for (final BC_NAME_SIZE obj : entry.chunk.files) {
        final String dst = entry.build_worker_path(obj.name);
        final File dbfile = new File(dst);
        //        System.out.println("  dst is:" + dst);
        //        System.out.println("  Size is " + dbfile.length());
        if (entry.present()) assertEquals(dbfile.length(), obj.size.longValue());
        // else assertEquals(dbfile.length(), 0);
        errors = entry.download(errorList, infoList);
        assertTrue(entry.present());
        assertTrue(dbfile.exists());
        assertEquals(dbfile.length(), obj.size.longValue());
      }
    }
    System.out.println("Finished");
  }

  public void run() {
    System.out.println("Thread started");
    setUpEnv();
    tempfolder = new TemporaryFolder();
    stress_dbload();
  }

  // Invoke with:
  // java -cp
  // .:/usr/local/spark/2.3.2/jars:target/sparkblast-1-jar-with-dependencies.jar:target/test-classes:$HOME/.m2/repository/junit/junit/4.12/junit-4.12.jar gov.nih.nlm.ncbi.blastjni.Test_BC_DATABASE_RDD_ENTRY
  public static void main(final String[] args) throws Exception {
    for (int i = 0; i != 30; ++i) {
      Test_BC_DATABASE_RDD_ENTRY thrd = new Test_BC_DATABASE_RDD_ENTRY();
      thrd.start();
      while (java.lang.Thread.activeCount() >= 8) {
        Thread.sleep(100);
      }
    }
  }
}
