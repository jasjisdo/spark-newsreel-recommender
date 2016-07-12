package de.dailab.newsreel.recommender.common.enums;

/**
 * Integer itemID = JsonPath.read(jsonMessage, "$.context.simple.25");
 * Long domainID  = JsonPath.read(jsonMessage, "$.context.simple.27");
 * Integer userID = JsonPath.read(jsonMessage, "$.context.simple.57");
 *
 * Created by domann on 11.12.15.
 */
public enum Simple {

    ITEM_ID("$.context.simple.25", "/context/simple/25"),
    DOMAIN_ID("$.context.simple.27", "/context/simple/27"),
    USER_ID("$.context.simple.57", "/context/simple/57"),
    CATEGORY("$.context.simple.11", "/context/simple/11");

    private final String jasonPath;
    private final String jacksonPath;

    public String jasonPath() { return jasonPath; }
    public String jacksonPath() { return jacksonPath; }

    Simple(String jasonPath, String jacksonPath) {
        this.jasonPath = jasonPath;
        this.jacksonPath = jacksonPath;
    }

}
