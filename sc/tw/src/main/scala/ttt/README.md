# TTT
## Usage

### Files
Loading the raw JSONL files as  `Dataset[_]` from the stream. 
 Currently twitter4j and twarc data is supported. 

```scala
import org.lamastex.mep.tw.ttt.TTTConverters._

val twarcTTTdS: Dataset[TTT] = spark.read.twarcToTTT("path/to/twarc_schema.json","/path/to/twarc.jsonl")
val twarcTTTURLsAndHashtagsdS: Dataset[TTTURLsAndHashtags] = spark.read.twarcToTTTURLsAndHashtags("path/to/twarc_schema.json","/path/to/twarc.jsonl")
val twarcTTTRTLikesAndMedia: Dataset[TTTRTLikesAndMedia] = spark.read.twarcToTTTRTLikesAndMedia("path/to/twarc_schema.json","/path/to/twarc.jsonl")
val twitter4jTTTdS: Dataset[TTT] = spark.read.twitter4jToTTT("path/to/twitter4j_schema.json","/path/to/twitter4j.jsonl")
val twitter4jTTTdS: Dataset[TTTURLsAndHashtags] = spark.read.twitter4jToTTTURlsAndHashtags("path/to/twitter4j_schema.json","/path/to/twitter4j.jsonl")
```