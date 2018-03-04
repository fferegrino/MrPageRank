package pageRank.dataTypes

@SerialVersionUID(982L)
class WikipediaRevision(val articleId: String, val revisionId: Long, val outlinks: String, val revisionTime: Long = Long.MaxValue) extends Serializable {
  override def toString = s"$articleId $revisionId outlinks: $outlinks"
}