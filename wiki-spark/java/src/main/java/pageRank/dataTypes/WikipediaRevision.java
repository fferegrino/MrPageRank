package pageRank.dataTypes;

import java.io.Serializable;

public class WikipediaRevision implements Serializable {
	private static final long serialVersionUID = 982L;
	
	
	
	public WikipediaRevision(String articleId, Long revisionId, String outlinks) {
		super();
		this.articleId = articleId;
		this.revisionId = revisionId;
		this.outlinks = outlinks;
	}
	
	@Override
	public String toString() {
		return articleId + " " + String.valueOf(revisionId) + " outlinks: " + outlinks;
	}

	private String articleId;
	public String getArticleId() {
		return articleId;
	}
	public void setArticleId(String articleId) {
		this.articleId = articleId;
	}
	
	private Long revisionId;
	public Long getRevisionId() {
		return revisionId;
	}
	public void setRevisionId(Long revisionId) {
		this.revisionId = revisionId;
	}
	
	private String outlinks;
	public String getOutlinks() {
		return outlinks;
	}
	public void setOutlinks(String outlinks) {
		this.outlinks = outlinks;
	}
}
