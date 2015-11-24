---
layout: post
title: Maven repositories moved
description: Maven repositories moved
modified: 2014-09-15
tags: [jasdb, news, release, website, nosql, document, storage, java, maven, central, repository, opensource, roadmap]
comments: false
---

As Announced in our other news post we have a fully new web site and also with that are moving our maven artefacts around. In order
to facilitate this process we have already moved our current and legacy artefacts to a github repository for safe keeping. All our new artefacts
will be release to maven central and the 1.0 version will be available in the coming weeks.

If you at this moment want to use the 0.8 or below versions of JasDB please use the below maven repository:
{% highlight xml %}
<repository>
  <id>jasdb-release</id>
  <name>JasDB release repository</name>
  <url>https://raw.github.com/oberasoftware/jasdb_release/mvn-repo</url>
</repository>
{% endhighlight %}

All the artefact ID's and Group ID's have remained the same, appologies for the incovenience but it is best to move these to Github for now.
