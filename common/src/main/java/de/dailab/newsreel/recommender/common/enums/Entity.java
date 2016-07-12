package de.dailab.newsreel.recommender.common.enums;

/**
 * Created by domann on 18.12.15.
 */
public enum Entity {

    ITEM_ID("$.itemID", ""),
    DOMAIN_ID("$.domainID", ""),
    USER_ID("$.userID", "");

    private final String jasonPath;
    private final String jacksonPath;

    public String jasonPath() { return jasonPath; }
    public String jacksonPath() { return jacksonPath; }

    Entity(String jasonPath, String jacksonPath) {
        this.jasonPath = jasonPath;
        this.jacksonPath = jacksonPath;
    }

}
