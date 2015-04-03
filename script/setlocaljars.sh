#!/bin/sh

home=`cd ~ && pwd`
mvn install:install-file -Dfile=../lib/fanlib1.0.0.jar -DgroupId=fan -DartifactId=fanlib -Dversion=1.0.0 -Dpackaging=jar -DlocalRepositoryPath=$home/.m2/repository/

mvn install:install-file -Dfile=../lib/segutil1.0.0.jar -DgroupId=fan -DartifactId=segutil -Dversion=1.0.0 -Dpackaging=jar -DlocalRepositoryPath=$home/.m2/repository/

mvn install:install-file -Dfile=../lib/weibo1.0.0.jar -DgroupId=fan -DartifactId=weibo -Dversion=1.0.0 -Dpackaging=jar -DlocalRepositoryPath=$home/.m2/repository/

mvn install:install-file -Dfile=../lib/IKAnalyzer2012_u6.jar -DgroupId=ikanalyzer -DartifactId=ikanalyzer -Dversion=2012_u6 -Dpackaging=jar -DlocalRepositoryPath=$home/.m2/repository/