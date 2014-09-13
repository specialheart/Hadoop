package hdfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class App2 {
	// FileSystem
	public static String PATH = "hdfs://localhost:9000";
	public static String DIR = "/d1";
	public static String FILE = "/d1/hello";

	public static void main(String[] args) throws IOException,
			URISyntaxException {

		FileSystem fileSystem = getFileSystem();

		// create dir -- hadoop fs -mkdir
		mkdir(fileSystem);

		// upload file -- hadoop fs -put src des
		putData(fileSystem);

		// download file -- hadoop fs -get src des
		getData(fileSystem);

		// read file
		list(fileSystem);

		// delete dir
		removeDir(fileSystem);
	}

	private static void list(FileSystem fileSystem) throws IOException {
		FileStatus[] listStatus = fileSystem.listStatus(new Path(DIR));
		for (FileStatus fileStatus : listStatus) {
			String isDir = fileStatus.isDir() ? "dir" : "file";
			String permission = fileStatus.getPermission().toString();
			short replication = fileStatus.getReplication();
			long len = fileStatus.getLen();
			String path = fileStatus.getPath().toString();
			System.out.println(isDir + "\t" + permission + "\t" + replication
					+ "\t" + len + "\t" + path + "\n");
		}
	}

	private static void getData(FileSystem fileSystem) throws IOException {
		FSDataInputStream in = fileSystem.open(new Path(FILE));
		IOUtils.copyBytes(in, System.out, 1024, true);
	}

	private static void putData(FileSystem fileSystem) throws IOException,
			FileNotFoundException {
		FSDataOutputStream out = fileSystem.create(new Path(FILE));
		FileInputStream in = new FileInputStream(new File(
				"/home/hadoop/Downloads/readme.txt"));
		IOUtils.copyBytes(in, out, 1024, true);
	}

	private static void removeDir(FileSystem fileSystem) throws IOException {
		fileSystem.delete(new Path(DIR), true);
	}

	private static void mkdir(FileSystem fileSystem) throws IOException {
		fileSystem.mkdirs(new Path(DIR));
	}

	private static FileSystem getFileSystem() throws IOException,
			URISyntaxException {
		return FileSystem.get(new URI(PATH), new Configuration());
	}
}
