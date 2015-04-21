organization := "com.sdhu"

name := "elasticsearchprediction-spark"

version := "0.1"

scalaVersion := "2.10.4"

val sparkV = "1.3.0"

scalacOptions ++= Seq(
  "-deprecation", 
  "-encoding", 
  "UTF-8", 
  "-feature", 
  "-unchecked",
  "-Ywarn-adapted-args", 
  "-Ywarn-value-discard", 
  "-Xlint")

resolvers ++= Seq(
  "Typesafe Releases" at "http://repo.typesafe.com/typesafe/releases/",
  "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkV,
  "org.apache.spark" %% "spark-mllib" % sparkV,
  ("org.elasticsearch" %% "elasticsearch-spark" % "2.1.0.Beta3").
    exclude("org.apache.hadoop", "hadoop-yarn-api").
    exclude("org.eclipse.jetty.orbit", "javax.mail.glassfish").
    exclude("org.eclipse.jetty.orbit", "javax.servlet").
    exclude("org.slf4j", "slf4j-api"),
  "com.holdenkarau" %% "spark-testing-base" % "1.3.0_0.0.5",
  "org.scalatest"       %% "scalatest"              % "2.2.0" % "test" withSources()
)


//excluding all the mess that causes the build to break on spark...
mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
  {
    case PathList("org", "apache", xs @ _*) => MergeStrategy.first
    case PathList("com","google", "common", xs @ _*) => MergeStrategy.last
    case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
    case x if x.startsWith("META-INF") => MergeStrategy.discard
    case x if x.endsWith(".html") => MergeStrategy.discard
    case x if x.startsWith("plugin.properties") => MergeStrategy.last
    case x => old(x)
  }
}

licenses := Seq("Apache License 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.html"))

//logLevel := Level.Warn

//logLevel in compile := Level.Warn

cancelable := true

parallelExecution in ThisBuild := false
