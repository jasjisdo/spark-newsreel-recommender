package de.dailab.newsreel.recommender.metarecommender.jsonpath;

import com.jayway.jsonpath.JsonPath;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * Created by domann on 07.12.15.
 */
public class JsonPathTest {

    private static final Logger log = Logger.getLogger(JsonPathTest.class);
    // static {log.setLevel(org.apache.log4j.Level.DEBUG);}

    private String json;

    @Before
    public void setUp() throws Exception {
        json = "{\n" +
                "    \"store\": {\n" +
                "        \"book\": [\n" +
                "            {\n" +
                "                \"category\": \"reference\",\n" +
                "                \"author\": \"Nigel Rees\",\n" +
                "                \"title\": \"Sayings of the Century\",\n" +
                "                \"price\": 8.95\n" +
                "            },\n" +
                "            {\n" +
                "                \"category\": \"fiction\",\n" +
                "                \"author\": \"Evelyn Waugh\",\n" +
                "                \"title\": \"Sword of Honour\",\n" +
                "                \"price\": 12.99\n" +
                "            },\n" +
                "            {\n" +
                "                \"category\": \"fiction\",\n" +
                "                \"author\": \"Herman Melville\",\n" +
                "                \"title\": \"Moby Dick\",\n" +
                "                \"isbn\": \"0-553-21311-3\",\n" +
                "                \"price\": 8.99\n" +
                "            },\n" +
                "            {\n" +
                "                \"category\": \"fiction\",\n" +
                "                \"author\": \"J. R. R. Tolkien\",\n" +
                "                \"title\": \"The Lord of the Rings\",\n" +
                "                \"isbn\": \"0-395-19395-8\",\n" +
                "                \"price\": 22.99\n" +
                "            }\n" +
                "        ],\n" +
                "        \"bicycle\": {\n" +
                "            \"color\": \"red\",\n" +
                "            \"price\": 19.95\n" +
                "        }\n" +
                "    },\n" +
                "    \"expensive\": 10\n" +
                "}awefhqwehfq324ifo32hfo";
    }

    @Test
    public void whenJsonPathQueriesAllAuthors_thenNoExceptions() throws Exception {
        List<String> authors = JsonPath.read(json, "$.store.book[*].author");
        assertThat(authors.size(), is(4));
        log.info(authors);
        assertThat(authors.get(0), is("Nigel Rees"));
        assertThat(authors.get(1), is("Evelyn Waugh"));
        assertThat(authors.get(2), is("Herman Melville"));
        assertThat(authors.get(3), is("J. R. R. Tolkien"));
    }
}
