name := "Tal_Distribute_Azkaban"

version := "0.1"

scalaVersion := "2.11.8"


libraryDependencies ++= Seq(
	"org.scalacheck" %% "scalacheck" % "1.13.4" % "test", //%%是scala的第三方依赖scala存在版本不兼容的问题
	"org.pegdown" % "pegdown" % "1.6.0" % "test", //% 是JAVA的第三方依赖
	"org.scalatest" %% "scalatest" % "3.0.1" % "test",
	"org.slf4j" % "slf4j-api" % "1.7.7",
	"ch.qos.logback" % "logback-classic" % "1.1.2",
	"com.typesafe.scala-logging" %% "scala-logging" % "3.7.1",
	"com.github.pathikrit" %% "better-files" % "3.4.0" // scala io
)

// append several options to the list of options passed to the Java compiler
javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

// append -deprecation to the options passed to the Scala compiler
scalacOptions += "-deprecation"

// only use a single thread for building
parallelExecution := true

// create beautiful scala test report
testOptions in Test ++= Seq(
	Tests.Argument(TestFrameworks.ScalaTest,"-h","target/html-unit-test-report"),
	Tests.Argument(TestFrameworks.ScalaTest,"-u","target/unit-test-reports"),
	Tests.Argument(TestFrameworks.ScalaTest,"-o"),
	Tests.Argument(TestFrameworks.ScalaTest,"-l","FunctionTest")
)

// mainClass in assembly := Some("com.tal.wuyuhang.MainApp")