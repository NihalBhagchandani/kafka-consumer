
name := """consumer"""


scalaVersion := "2.11.7"

resolvers += "Apache repo" at "https://repository.apache.org/content/repositories/releases"

libraryDependencies ++= Seq(         
		            "org.apache.kafka" %%  "kafka"                   %  "0.8.2.1" exclude("org.slf4j","*"),
		            "org.apache.spark" %%  "spark-core"              %  "1.4.1"   ,
		            "org.json4s"       %%  "json4s-native"           %  "3.2.10"  ,
		            "org.apache.spark" %   "spark-hive_2.11"         %  "1.4.1"   ,
		            "org.apache.spark" %   "spark-streaming_2.11"    %  "1.4.1"   ,
		            "org.ahocorasick"  %   "ahocorasick"             %  "0.2.4"   ,
		            "ch.qos.logback"   %   "logback-classic"         %  "1.0.13"
)

