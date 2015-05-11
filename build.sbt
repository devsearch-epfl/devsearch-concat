name := """devsearch-concat"""

shellPrompt := { state => "[\033[36m" + name.value + "\033[0m] $ " }

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % "2.3.9",
  "com.typesafe.akka" %% "akka-testkit" % "2.3.9" % "test",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test",
  "com.github.scopt" %% "scopt" % "3.3.0",
  "org.apache.tika" % "tika-core" % "1.7",
  "de.sven-jacobs" % "loremipsum" % "1.0",
  "org.apache.commons" % "commons-compress" % "1.9"
)

resolvers += Resolver.sonatypeRepo("public")

target in Compile in doc := baseDirectory.value / "api"
