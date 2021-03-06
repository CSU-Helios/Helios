#!/bin/bash
MONGODBPATH=data/db
MONGODBLOG=log/mongodb.log
MONGODBPORT=27017

if [ -z "$1" ] ; then
	echo -e "[-mongod]"
	echo -e "\t[-start]\t: start mongod at localhost 27017"
	echo -e "\t[-stop] \t: stop mongod at localhost 27017"
	echo -e "\t[-show] \t: show onrunning mongod processes"
	
	echo -e "[-mongo]"
	echo -e "\t[-start]\t: start mongo shell"
	
	echo -e "[-python]"
	echo -e "\t[-start]\t: start local python shell"
	echo -e "\t[-Helios]\t: run Helios python class"
   
        echo -e "[-server]"
        echo -e "\t[-start]\t: start helios server"
        echo -e "\t[-stop] \t: stop helios server"
fi
	
if [ "$1" == "-mongod" ] ; then
    if [ "$2" == "-start" ] ; then
        softwares/mongodb-linux-x86_64-3.6.2/bin/mongod --dbpath $MONGODBPATH --logpath $MONGODBLOG --port $MONGODBPORT &
        echo -e "******************************************************************************************"
        echo -e "Starting mongod at PORT $MONGODBPORT, dbpath=$MONGODBPATH, logpath=$MONGODBLOG 64-bit"
        echo -e "******************************************************************************************"
	cat $MONGODBLOG
    fi

    if [ "$2" == "-stop" ] ; then
        softwares/mongodb-linux-x86_64-3.6.2/bin/mongod --shutdown
        echo -e "******************************************************************************************"
        echo -e "Stopping Mongod"
        echo -e "******************************************************************************************"
    fi

    if [ "$2" == "-show" ] ; then
        ps -xa | grep mongod
    fi
fi


if [ "$1" == "-mongo" ] ; then
    if [ "$2" == "-start" ] ; then
        mongo --port $MONGODBPORT
    fi
fi


if [ "$1" == "-python" ] ; then
    if [ "$2" == "-start" ] ; then
        softwares/Python-3.6.4/python   
    fi
	
    if [ "$2" == "-Helios" ] ; then
        if [ -z "$3" ]; then
            softwares/Python-3.6.4/python Helios.py -h
        else
            softwares/Python-3.6.4/python Helios.py $3 $4 $5 $6 $7 $8 $9 $a $b $c $d $e
        fi
    fi
fi

if [ "$1" == "-server" ]; then
    if [ "$2" == "-start" ]; then
        softwares/apache-tomcat-9.0.6/bin/catalina.sh stop
        javac -d heliosServlet/WEB-INF/classes heliosServlet/WEB-INF/src/helios/*/*.java -cp heliosServlet/WEB-INF/lib/javax.json-1.0.4.jar:heliosServlet/WEB-INF/lib/javax.json-api-1.0.jar:heliosServlet/WEB-INF/lib/mongo-java-driver-3.6.3.jar:heliosServlet/WEB-INF/lib/servlet-api.jar
        softwares/apache-tomcat-9.0.6/bin/catalina.sh run &
    fi

    if [ "$2" == "-stop" ]; then
        softwares/apache-tomcat-9.0.6/bin/catalina.sh stop
    fi
fi
