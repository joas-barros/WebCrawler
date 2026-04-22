package model;

import utils.Color;
import utils.Labels;

import java.io.Serializable;
import java.util.List;

public class WebSite implements Serializable {
    private static final long serialVersionUID = 1L;

    private String url;
    private String contentHTML;
    private List<String> links;
    private Labels label;

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

    public Labels getLabel() { return label; }

    public void setLabel(Labels label) { this.label = label; }
}
