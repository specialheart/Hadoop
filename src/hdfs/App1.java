package hdfs;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

/**
 * use URL to access HDFS
 * 
 * @author root
 *
 */
public class App1 {
	public static String PATH = "hdfs://localhost:9000/hello";

	public static void main(String[] args) throws IOException {
		// hadoop fs -ls /
		// hadoop fs -ls hdsf://localhost:9000
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
		URL url = new URL(PATH);

		InputStream in = url.openStream();
		IOUtils.copyBytes(in, System.out, 1024, true);

	}
}
