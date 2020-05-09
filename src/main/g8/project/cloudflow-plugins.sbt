import sbt. _
resolvers += "Akka Snapshots" at "https://repo.akka.io/snapshots/"
resolvers += Resolver.url("cloudflow", url("https://dl.bintray.com/pencilerazzzer/cloudflow"))(Resolver.ivyStylePatterns)
addSbtPlugin("com.lightbend.cloudflow" % "sbt-cloudflow" % "1.3.4-pre-7-910621c")