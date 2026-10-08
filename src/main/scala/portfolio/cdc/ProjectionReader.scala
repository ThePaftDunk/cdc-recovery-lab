package portfolio.cdc

import java.sql.DriverManager

/** A separate read adapter: no DDL, repairs, checkpoint writes or quarantine reads. */
object ProjectionReader {
  def read(url: String, user: String, password: String): Vector[ProjectionRow] = {
    val readUrl = if (url.startsWith("jdbc:h2:")) {
      Class.forName("org.h2.Driver")
      // Even opening a missing H2 file must not create a database during diagnosis.
      url + ";IFEXISTS=TRUE"
    } else {
      require(url.startsWith("jdbc:postgresql:"), "reconciliation supports H2 or PostgreSQL")
      Class.forName("org.postgresql.Driver")
      url
    }
    val connection = DriverManager.getConnection(readUrl, user, password)
    try {
      connection.setReadOnly(true)
      connection.setAutoCommit(false)
      val statement = connection.prepareStatement("SELECT entity_key, body FROM projection ORDER BY entity_key")
      try {
        statement.setMaxRows(Reconciliation.MaxRows + 1)
        statement.setFetchSize(100)
        statement.setQueryTimeout(10)
        val result = statement.executeQuery()
        val rows = Vector.newBuilder[ProjectionRow]
        var count = 0
        try while (result.next()) {
          count += 1
          require(count <= Reconciliation.MaxRows, s"target exceeds ${Reconciliation.MaxRows} rows; a chunked reconciler is required")
          rows += ProjectionRow(result.getString(1), ujson.read(result.getString(2)))
        } finally result.close()
        rows.result()
      } finally statement.close()
    } finally {
      try connection.rollback() finally connection.close()
    }
  }
}
