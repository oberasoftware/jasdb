---
layout: post
title: JasDB v0.8 Snapshots
description: JasDB v0.8 Snapshots
modified: 2013-02-28
tags: [jasdb, development, snapshots]
---

It has been a while since we last posted on OberaSoftware.com. We have been very busy with other projects alongside JasDB, but want you to know that we have not forgotten about JasDB. On the contrary, we are currently busy with the 0.8 version of JasDB, which contains some new and exciting features:

* New fully-in-memory mapped record storage
* API Improvements allowing instance management through the API
* Default configuration for JasDB, no configuration required to launch
* Java embedded in-process support improvements
* Embedded support improved for ARM-based platforms
* User management
* REST OAuth support
* Inverted index merged into Btree
* Inverted index speed now on same level as Btree
* BTree index speed improvements
* Significantly reduced disk size for indexes
* Performance improvements

We have seen significant speed improvements in the new version of JasDB, with up to 80k inserts/sec using 20 threads on moderate hardware (4 core 2.3GHZ core i7 with a 256gb ssd). This is up from a speed of 27k inserts/sec using the 0.7.1 version of JasDB.

Development snapshots

Please stay tuned as we keep you up-to-date on more improvements in the coming months. We aim to release the 0.8 version around the end of May 2013. You can get your download of the development snapshot on the downloads page. All maven artifacts are available on the 0.8-SNAPSHOT (see Snapshot repository configuration) artifact id.

That’s all for now, from us at OberaSoftware!
