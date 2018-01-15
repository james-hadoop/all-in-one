package selenium;

import com.thoughtworks.selenium.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

@SuppressWarnings("deprecation")
public class SeleniumTestDemo extends SeleneseTestCase {
    @Before
    public void setUp() throws Exception {
        selenium = new DefaultSelenium("localhost", 4444, "*iexplore", "http://www.baidu.com/");
        selenium.start();
    }

    @Test
    public void testTest() throws Exception {
        selenium.open("/");
        selenium.type("id=kw", "aaaa");
        selenium.click("id=su");
    }

    @After
    public void tearDown() throws Exception {
        selenium.stop();
    }
}
