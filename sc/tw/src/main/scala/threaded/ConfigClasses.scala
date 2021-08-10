package org.lamastex.mep.tw

case class StreamConfig (
  handlesFilePath: String,
  streamDuration: Long
)

case class WriteConfig (
  outputFilenames: String,
  fullFilesDirectory: String,
  maxFileSize: Long,
  writeRate: Long
)

case class UpdateConfig (
  updateRate: Long
)

case class TweetSchema (
  id: Long, 
  json: String, 
  time: Long,
  userID: Long,
  statusType: String
)
