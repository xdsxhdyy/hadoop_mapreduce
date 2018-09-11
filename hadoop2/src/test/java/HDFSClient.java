
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


public class HDFSClient {

    @Test
    public void testMkdir() throws URISyntaxException, IOException, InterruptedException {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.149.20:8020"), configuration, "admin");
        fs.mkdirs(new Path("/user/admin/ly51"));
        fs.close();

    }

}
