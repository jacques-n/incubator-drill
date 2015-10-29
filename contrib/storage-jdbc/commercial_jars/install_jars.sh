#!/bin/bash
mvn install:install-file -DgroupId=com.oracle -DartifactId=ojdbc7 -Dversion=12.1.0.2 -Dpackaging=jar -Dfile=ojdbc7.12.1.0.2.jar -DgeneratePom=true
mvn install:install-file -DgroupId=com.oracle -DartifactId=mysql.jdbc -Dversion=5.1.37 -Dpackaging=jar -Dfile=mysql-connector-java-5.1.37-bin.jar -DgeneratePom=true 
mvn install:install-file -DgroupId=com.microsoft -DartifactId=sqljdbc41 -Dversion=4.2.6420.100 -Dpackaging=jar -Dfile=sqljdbc41.4.2.6420.100.jar -DgeneratePom=true
