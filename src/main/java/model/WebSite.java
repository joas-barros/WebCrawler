package model;

import java.util.List;

public class WebSite {
    private String url;
    private String contentHTML;
    private List<String> links;

    public WebSite(String url, String contentHTML, List<String> links) {
        this.url = url;
        this.contentHTML = contentHTML;
        this.links = links;
    }

    public WebSite() {
    }

    public String getUrl() {
        return url;
    }

    public String getContentHTML() {
        return contentHTML;
    }

    public List<String> getLinks() {
        return links;
    }
}
