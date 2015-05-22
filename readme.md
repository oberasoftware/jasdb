JasDB is an open Document based database that can be used to store
unstructured data. The database can be used standalone and accessed
through a java client library or the REST interface.

Also the database can be used in process of your Java application,
ideal for embedded projects (including on Android) where a small lightweight database is needed.

## Features
JasDB has the folowing features:
* Lightweight memory and cpu profile
* High throughput on a single machine
* Full query capabilities
* BTree index structure
* REST webservice
* Android support
* Java client API (both for remote and local embedded mode)

## Documentation
For more information have a look at http://www.oberasoftware.com

Wiki for installation and API usage: https://github.com/oberasoftware/jasdb-open/wiki
Javadoc: http://oberasoftware.github.io/jasdb/apidocs/

## Quick Installation
1. Install JasDB by unzipping the download
2. Start the database using start.bat or start.sh
3. Open http://localhost:7050

For more details see here: https://github.com/oberasoftware/jasdb-open/wiki/Installing-and-configuring-JasDB

### Java Example
```
//Open DB Session
DBSession session = new LocalDBSession();
EntityBag bag = session.createOrGetBag("MyBag");

//Insert some data
SimpleEntity entity = new SimpleEntity();
entity.addProperty("title", "Title of my content");
entity.addProperty("text", "Some big piece of text content");
bag.addEntity(entity);

//Retrieve entity by Id
SimpleEntity e = bag.getEntity("056f8058-e1f7-4f8e-a2f8-332e62c15961");

//Query the DB
QueryExecutor executor = bag.find(QueryBuilder.createBuilder().field("field").value(queryKey));
QueryResult result = executor.execute();
for(SimpleEntity entity : result) {
   //access the properties if desired
   entity.getProperty("field1");
}
```

For more information:
* WIKI: https://github.com/oberasoftware/jasdb-open/wiki/Java-Client-API
* JavaDoc API: http://oberasoftware.github.io/jasdb/apidocs/

## License
This software adheres to the MIT X11 license:
Copyright (c) 2014 Obera Software

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

There is an additional requirement to the above MIT X11 License:
Redistribution of this software in source or binary forms shall be free of all charges or fees to the recipient of this software.
