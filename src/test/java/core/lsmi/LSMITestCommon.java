package core.lsmi;

import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.PropertyConfigurator;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import core.lsmt.LSMTInvertedIndex;
import util.Configuration;

public class LSMITestCommon {
	protected static final Logger logger = LoggerFactory.getLogger(ReaderWriterTest.class);
	protected LSMTInvertedIndex index;
	protected Configuration conf;

	@Before
	public void setupIndex() throws IOException {
		PropertyConfigurator.configure("conf/log4j-server.properties");
		conf = new Configuration();
		conf.load("conf/index.conf_template");
		FileUtils.deleteDirectory(conf.getIndexDir());
		conf.getIndexDir().mkdirs();
		conf.getTmpDir().mkdirs();
		index = new LSMTInvertedIndex(conf);
	}

	@After
	public void cleanUp() throws IOException {
		FileUtils.deleteDirectory(conf.getIndexDir());
	}

}
