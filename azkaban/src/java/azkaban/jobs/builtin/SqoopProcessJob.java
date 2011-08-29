package azkaban.jobs.builtin;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import azkaban.app.JobDescriptor;

/**
 * Sqoop Job
 * 
 */
public class SqoopProcessJob extends JavaProcessJob {
	public static final String SQOOP_COMMAND = "sqoop.command";
	public static final String SQOOP_ARGS = "sqoop.args";

	public static final String SQOOP_JAVA_CLASS = "com.cloudera.sqoop.Sqoop";

	/**
	 * @param descriptor
	 */
	public SqoopProcessJob(JobDescriptor descriptor) {
		super(descriptor);
	}

	@Override
	protected String getJavaClass() {
		return SQOOP_JAVA_CLASS;
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

		// Add sqoop home setting.
		String sqoopHome = System.getenv("SQOOP_HOME");
		if (sqoopHome == null) {
			info("SQOOP_HOME not set, using default sqoop config.");
		} else {
			info("Using sqoop config found in " + sqoopHome);
			classPath.add(new File(sqoopHome, "conf").getPath());
		}

		return classPath;
	}

	@Override
	protected String getMainArguments() {
		ArrayList<String> list = new ArrayList<String>();
		
		String cmd = getSqoopCommand();
		if (cmd != null) {
			list.add(cmd);
		}
		
		String args = getSqoopArgs();
		if (args != null) {
			list.add(args);
		}
		
		return org.apache.commons.lang.StringUtils.join(list, " ");
	}
	
	protected String getSqoopCommand() {
		return getProps().getString(SQOOP_COMMAND, null);
	}
	
	protected String getSqoopArgs() {
		return getProps().getString(SQOOP_ARGS, null);
	}
}
