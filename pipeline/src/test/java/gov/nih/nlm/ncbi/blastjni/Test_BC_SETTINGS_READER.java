package gov.nih.nlm.ncbi.blastjni;

import org.junit.*;
import static org.junit.Assert.*;

import java.util.Map;
import java.util.List;


public class Test_BC_SETTINGS_READER {

    static public final String CFG_FILE_DFLT = "ini.json";

    static private String cfg_file;

    @BeforeClass
    public static void setUpEnv() {
        Map<String, String> env = System.getenv();
        cfg_file = (String) env.getOrDefault("CFG_FILE", CFG_FILE_DFLT);
    }

    @Test
    public void test_read_from_file() {
        System.out.println("Using config file: " + cfg_file);
        BC_SETTINGS res = BC_SETTINGS_READER.read_from_json(cfg_file, "junit");

        /* REQUESTS */

        assertTrue ("one of req_use_pubsub, req_use_files, req_use_socket has to be true",
                    (res.req_use_pubsub == true || res.req_use_files == true || res.req_use_socket == true));

        if (res.req_use_pubsub == true) {
            assertTrue ("if req_use_pubsub == true, then req_use_files and req_use_socket must be false",
                        (res.req_use_files == false || res.req_use_socket == false));

            assertTrue ("if req_use_pubsub == true, then req_pubsub_project_id and req_pubsub_subscript_id must be set",
                        (!res.req_pubsub_project_id.isEmpty() && !res.req_pubsub_subscript_id.isEmpty()));
        }

        if (res.req_use_files == true) {
            assertTrue ("if req_use_files == true, then req_use_pubsub and req_use_socket must be false",
                        (res.req_use_pubsub == false || res.req_use_socket == false));

            assertTrue ("if req_use_files == true, then req_files_dir must be set",
                        (!res.req_files_dir.isEmpty()));
        }

        if (res.req_use_socket == true) {
            assertTrue ("if req_use_socket == true, then req_use_pubsub and req_use_files must be false",
                        (res.req_use_pubsub == false || res.req_use_files == false));

            assertTrue ("if req_use_socket == true, then req_port_nr must be set",
                        (res.req_port_nr > 0));
        }

        assertTrue ("req_max_backlog must be > 0", (res.req_max_backlog > 0));

        /* DATABASES */

        assertTrue ("dbs must not be empty", !res.dbs.isEmpty());

        /* RESULTS */

        assertTrue ("one of res_use_gs_bucket or res_use_files has to be true",
                    (res.res_use_gs_bucket == true || res.res_use_files == true));

        if (res.res_use_gs_bucket == true) {
            assertTrue ("if res_use_gs_bucket == true, then res_use_files must be false",
                        (res.res_use_files == false));

            assertTrue ("if res_use_gs_bucket == true, then res_gs_bucket and res_gs_pattern must be set",
                        (!res.res_gs_bucket.isEmpty() && !res.res_gs_pattern.isEmpty()));
        }

        if (res.res_use_files == true) {
            assertTrue ("if res_use_files == true, then res_use_gs_bucket must be false",
                        (res.res_use_gs_bucket == false));

            assertTrue ("if res_use_files == true, then res_files_dir and res_files_pattern must be set",
                        (!res.res_files_dir.isEmpty() && !res.res_files_pattern.isEmpty()));
        }

        /* CLUSTER */
        assertTrue ("transfer_files must be set", (!res.transfer_files.isEmpty()));
        assertTrue ("num_executors must be > 0", (res.num_executors > 0));
        assertTrue ("num_executor_cores must be > 0", (res.num_executor_cores > 0));
        assertTrue ("parallel_jobs must be > 0", (res.parallel_jobs > 0));

        /* SUMMARY CHECK */
        assertTrue ("Summary check", res.valid());

        System.out.println(res.toString());

    }
}
