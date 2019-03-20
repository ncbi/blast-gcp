package gov.nih.nlm.ncbi.blastjni;

import static org.junit.Assert.*;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

public class Test_BC_DATABASE_RDD_ENTRY {

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
    final BC_SETTINGS res = BC_SETTINGS_READER.read_from_json(cfg_file, "junit");
    assertTrue("dbs must not be empty", !res.dbs.isEmpty());

    final String key = "nr";
    final BC_DATABASE_SETTING setting = res.dbs.get(key);
    final File tmpfol = tempfolder.newFolder("db");
    setting.worker_location = tmpfol.getAbsolutePath();

    final List<BC_NAME_SIZE> files = BC_GCP_TOOLS.list(setting.source_location);

    final List<BC_CHUNK_VALUES> allChunks =
        BC_GCP_TOOLS.unique_by_extension(files, setting.extensions);

    final List<BC_DATABASE_RDD_ENTRY> entries =
        BC_DATABASE_RDD_ENTRY.make_rdd_entry_list(setting, allChunks);
    final List<String> errorList = new ArrayList<>();
    final List<String> infoList = new ArrayList<>();
    boolean errors;
    final int part = rng.nextInt(entries.size());
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
    errors = entry.download(errorList,infoList);
    //assertFalse(errors);
    assertTrue(entry.present());
    for (final BC_NAME_SIZE obj : entry.chunk.files) {
      final String dst = entry.build_worker_path(obj.name);
      final File dbfile = new File(dst);
      assertTrue(dbfile.exists());
      assertEquals(dbfile.length(), obj.size.longValue());
    }
  }

  public static void main(final String[] args) throws Exception {}
}
