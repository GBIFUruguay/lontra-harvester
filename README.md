#lontra-harvester
* Current stable version: 1.3

Harvester used to feed the data portals based on [canadensys-explorer](https://github.com/Canadensys/canadensys-explorer) from occurrence record resources structured within the Darwin Core Archive standard. Part of the ingested data is processed using the [narwhal-processor](https://github.com/Canadensys/narwhal-processor).

Code Status
-----------
[![Build Status](https://travis-ci.org/WingLongitude/lontra-harvester.png)](https://travis-ci.org/WingLongitude/lontra-harvester)

Modules
-------
* Shared library [lontra-harvester-lib](https://github.com/WingLongitude/lontra-harvester/tree/master/lontra-harvester-lib)
* Java Swing Graphic User Interface [lontra-harvester-ui](https://github.com/WingLongitude/lontra-harvester/tree/master/lontra-harvester-ui)
* Multi-instantiable process that processes ActiveMQ messages[lontra-harvester-node](https://github.com/WingLongitude/lontra-harvester/tree/master/lontra-harvester-node)
* Command Line Interface [lontra-harvester-cli](https://github.com/WingLongitude/lontra-harvester/tree/master/lontra-harvester-cli)

Documentation
-------------
Visit our [wiki](https://github.com/WingLongitude/harvester/wiki)


Dependencies
------------
### Softwares
* [Postgresl](http://www.postgresql.org/)
* [ActiveMQ](http://activemq.apache.org/)
* [PostGIS](http://postgis.net/)

### Libraries
* [Liger Data Access](https://github.com/WingLongitude/liger-data-access)
* [Rome](https://github.com/rometools/rome)
* [Jackson](https://github.com/FasterXML/jackson)
* [Apache CLI](http://commons.apache.org/proper/commons-cli/)
