package de.dailab.newsreel.recommender.metarecommender.util;

import static de.dailab.newsreel.recommender.common.util.ConfigLoader.*;

import de.dailab.newsreel.recommender.common.util.ConfigLoader;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


/**
 * Created by jens on 10.12.15.
 */
public class ConfigLoaderTest {

    @Before
    public void setUp() throws Exception {

    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void testLoad() throws Exception {
        ConfigLoader.Conf conf = load();
        assert(!conf.getAppName().isEmpty());
        assert(!conf.getMaster().isEmpty());
    }
}