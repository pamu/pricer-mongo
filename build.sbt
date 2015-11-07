name := "PricerMongo"

version := "1.0"

scalaVersion := "2.11.7"

resolvers += "typesafe" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies ++= Seq(
  "org.mongodb" %% "casbah" % "2.8.2",
  "com.typesafe.play" %% "play-json" % "2.4.0",
  "org.slf4j" % "slf4j-log4j12" % "1.7.12"
)