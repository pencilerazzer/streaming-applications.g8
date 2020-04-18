// Resolver for the cloudflow-sbt plugin
//
resolvers += "Akka Snapshots" at "https://repo.akka.io/snapshots/"
resolvers += Resolver.bintrayRepo("pencilerazer","cloudflow")
resolvers += Resolver.url("cloudflow", url("https://pencilerazer.bintray.com/cloudflow"))(Resolver.ivyStylePatterns)

addSbtPlugin("com.lightbend.cloudflow" % "sbt-cloudflow" % "1.3.4-pre-225-c989b8d")