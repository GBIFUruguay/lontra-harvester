package net.canadensys.harvester;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import net.canadensys.Main;
import net.canadensys.harvester.config.CLIProcessingConfigTest;

import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

/**
 * See CLIProcessingConfigTest for Mock implementation used
 * 
 * @author cgendreau
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = CLIProcessingConfigTest.class, loader = AnnotationConfigContextLoader.class)
public class MainCLIJobTest {

	private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();

	@Before
	public void redirectSystemOut() {
		System.setOut(new PrintStream(outContent));
	}

	@After
	public void clean() {
		System.setOut(null);
	}

	/**
	 * We override the Main class to change the value of CONFIG_CLASS
	 * 
	 * @author cgendreau
	 *
	 */
	private static class MainTest extends Main {
		static {
			CONFIG_CLASS = CLIProcessingConfigTest.class;
		}

		public void mainOverride(String[] args) {
			main(new String[] { "-l" });
		}
	}

	@Test
	public void testListOption() {
		MainTest mainTest = new MainTest();
		mainTest.mainOverride(new String[] { "-l" });

		assertEquals("[1] QMOR", StringUtils.chomp(outContent.toString()));
		outContent.reset();
	}

}
