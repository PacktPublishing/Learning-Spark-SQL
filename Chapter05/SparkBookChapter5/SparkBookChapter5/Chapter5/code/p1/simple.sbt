name := "spark-streaming-example"

version := "1.0"

scalaVersion := "2.11.7"

resolvers += "jitpack" at "https://jitpack.io"

libraryDependencies ++= Seq("org.apache.spark" %% "spark-core"  % "2.2.0",
						"org.apache.spark" %% "spark-streaming" % "2.2.0",
						"org.apache.httpcomponents" % "httpclient" % "4.5.2",
						"org.scalaj" %% "scalaj-http" % "2.2.1",
  						"org.jfarcand" % "wcs" % "1.5")