package azkaban.jobs.builtin;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import azkaban.app.JobDescriptor;

/**
 * Hive Job
 * 
 */
public class HiveProcessJob extends JavaProcessJob {

	public static final String HIVE_SCRIPT = "hive.script";
	public static final String HIVE_CONFS_PREFIX = "hiveconf.";
	public static final String HIVE_INIT_FILE = "init.file";

	public static final String HIVE_CLIDRIVER_JAVA_CLASS = "org.apache.hadoop.hive.cli.CliDriver";

	public static final String HIVE_INITIAL_MEMORY_SIZE = "256M";
	public static final String HIVE_MAX_MEMORY_SIZE = "1024M";

	/**
	 * @param descriptor
	 */
	public HiveProcessJob(JobDescriptor descriptor) {
		super(descriptor);
	}

	@Override
	protected String getJavaClass() {
		return HIVE_CLIDRIVER_JAVA_CLASS;
	}

	@Override
	protected List<String> getClassPaths() {
		List<String> classPath = super.getClassPaths();
		
		// Add hadoop home setting.
		String hadoopHome = System.getenv("HADOOP_HOME");
		if (hadoopHome == null) {
			info("HADOOP_HOME not set, using default hadoop config.");
		} else {
			info("Using hadoop config found in " + hadoopHome);
			classPath.add(new File(hadoopHome, "conf").getPath());
		}

		// Add hive home setting.
		String hiveHome = System.getenv("HIVE_HOME");
		if (hiveHome == null) {
			info("HIVE_HOME not set, using default hive config.");
		} else {
			info("Using hive config found in " + hiveHome);
			classPath.add(new File(hiveHome, "conf").getPath());
		}

		return classPath;
	}

	@Override
	protected String getInitialMemorySize() {
		return HIVE_INITIAL_MEMORY_SIZE;
	}

	@Override
	protected String getMaxMemorySize() {
		return HIVE_MAX_MEMORY_SIZE;
	}

	@Override
	protected String getMainArguments() {
		ArrayList<String> list = new ArrayList<String>();

		String initFile = getHiveInitFile();
		if (initFile != null) {
			list.add("-i " + initFile);
		}
		
		list.add("-f " + getScript());
		
		Map<String, String> map = getHiveConfs();
		if (map != null) {
			for (Map.Entry<String, String> entry : map.entrySet()) {
				list.add("--hiveconf " + entry.getKey() + "=" + entry.getValue());
			}
		}

		return org.apache.commons.lang.StringUtils.join(list, " ");
	}

	protected Map<String, String> getHiveConfs() {
		return getProps().getMapByPrefix(HIVE_CONFS_PREFIX);
	}

	protected String getHiveInitFile() {
		return getProps().getString(HIVE_INIT_FILE, null);
	}

	protected String getScript() {
		return getProps().getString(HIVE_SCRIPT, getJobName() + ".q");
	}

}
