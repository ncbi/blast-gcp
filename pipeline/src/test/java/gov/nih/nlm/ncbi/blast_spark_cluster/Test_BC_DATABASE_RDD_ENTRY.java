package gov.nih.nlm.ncbi.blast_spark_cluster;

import org.junit.*;
import static org.junit.Assert.*;

import java.util.Map;
import java.util.List;
import java.util.ArrayList;


public class Test_BC_DATABASE_RDD_ENTRY {

    static public final String CFG_FILE_DFLT = "ini.json";

    static private String cfg_file;

    @BeforeClass
    public static void setUpEnv() {
        Map<String, String> env = System.getenv();
        cfg_file = (String) env.getOrDefault("CFG_FILE", CFG_FILE_DFLT);
    }

    @Test
    public void test_make_rdd_entry() {
        System.out.println("Using config file: " + cfg_file);
        BC_SETTINGS res = BC_SETTINGS_READER.read_from_json(cfg_file, "junit");

        assertTrue ("dbs must not be empty", !res.dbs.isEmpty());
        for (String key : res.dbs.keySet()) {
            BC_DATABASE_SETTING setting = res.dbs.get(key);
            List< String > names = new ArrayList();
            names.add(key);
            List< BC_DATABASE_RDD_ENTRY > entryList = BC_DATABASE_RDD_ENTRY.make_rdd_entry_list(setting, names);
            assertTrue ("entryList must not be empty", !entryList.isEmpty());
            for (BC_DATABASE_RDD_ENTRY entry : entryList ) {
                assertTrue ("list entry is not NULL", entry != null);
                assertTrue ("BC_DATABASE_SETTING is not NULL", entry.setting != null);
                assertTrue ("Name is not empty", !entry.name.isEmpty());
                System.out.println(entry.name);
                System.out.println(entry.setting.toString());
            }
        }

    }
}
