lazy val root = (project in file(".")).
  settings(
    exportJars := true,
    name := "sparkss",
    version := "1.0",
    scalaVersion := "2.10.6",
    libraryDependencies ++= Seq(
      "org.apache.spark" % "spark-core_2.10" % "1.5.2"
    )
  )

