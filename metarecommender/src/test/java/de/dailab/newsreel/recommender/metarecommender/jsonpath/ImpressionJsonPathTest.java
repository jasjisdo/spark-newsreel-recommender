package de.dailab.newsreel.recommender.metarecommender.jsonpath;

import com.jayway.jsonpath.JsonPath;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * Created by domann on 07.12.15.
 */
public class ImpressionJsonPathTest {

    private static final Logger log = Logger.getLogger(ImpressionJsonPathTest.class);
    // static {log.setLevel(org.apache.log4j.Level.DEBUG);}

    private String plistaJsonImpression;

    @Test
    public void testName() throws Exception {
        Integer viewedItemId = JsonPath.read(plistaJsonImpression, "$.context.simple.25");
        assertThat(viewedItemId, is(130731742));
        log.info(String.format("Id of viewed article is %s", viewedItemId));
    }

    @Before
    public void setUp() throws Exception {
        plistaJsonImpression = "{\n" +
                "    \"type\":\"impression\",\n" +
                "    \"context\":{\n" +
                "        \"simple\":{\n" +
                "            \"27\":418,\n" +
                "            \"25\":130731742,\n" +
                "            \"4\":312613,\n" +
                "            \"52\":0,\n" +
                "            \"14\":31721,\n" +
                "            \"19\":52193,\n" +
                "            \"24\":0,\n" +
                "            \"6\":431247,\n" +
                "            \"5\":86,\n" +
                "            \"47\":654013,\n" +
                "            \"18\":0,\n" +
                "            \"17\":48985,\n" +
                "            \"22\":65121,\n" +
                "            \"31\":0,\n" +
                "            \"13\":2,\n" +
                "            \"9\":26890,\n" +
                "            \"23\":17,\n" +
                "            \"57\":1331571080\n" +
                "        },\n" +
                "        \"lists\":{\n" +
                "            \"8\":[\n" +
                "                18841,\n" +
                "                18842,\n" +
                "                48511\n" +
                "            ],\n" +
                "            \"10\":[\n" +
                "                9,\n" +
                "                10\n" +
                "            ],\n" +
                "            \"11\":[\n" +
                "                2045611\n" +
                "            ]\n" +
                "        },\n" +
                "        \"clusters\":{\n" +
                "            \"33\":{\n" +
                "                \"82427\":11,\n" +
                "                \"8896\":7,\n" +
                "                \"33453554\":4,\n" +
                "                \"296087\":3,\n" +
                "                \"56332\":3,\n" +
                "                \"689251\":2,\n" +
                "                \"27499\":1,\n" +
                "                \"32941772\":1,\n" +
                "                \"70764\":1,\n" +
                "                \"17128\":0\n" +
                "            },\n" +
                "            \"2\":[\n" +
                "                12,\n" +
                "                13,\n" +
                "                42,\n" +
                "                90,\n" +
                "                46,\n" +
                "                29,\n" +
                "                19\n" +
                "            ],\n" +
                "            \"46\":{\n" +
                "                \"472419\":255,\n" +
                "                \"472358\":255,\n" +
                "                \"472441\":255\n" +
                "            },\n" +
                "            \"1\":{\n" +
                "                \"7\":255\n" +
                "            },\n" +
                "            \"3\":[\n" +
                "                43,\n" +
                "                24,\n" +
                "                44,\n" +
                "                105,\n" +
                "                20,\n" +
                "                16\n" +
                "            ]\n" +
                "        }\n" +
                "    },\n" +
                "    \"recs\":{\n" +
                "        \"ints\":{\n" +
                "            \"3\":[\n" +
                "                130106300,\n" +
                "                84799192\n" +
                "            ]\n" +
                "        }\n" +
                "    },\n" +
                "    ￼￼￼￼￼￼\"timestamp\":1372175999641\n" +
                "}";
    }
}
