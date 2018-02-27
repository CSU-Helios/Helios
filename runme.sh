#!/bin/bash
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
fi
	
if [ "$1" == "-mongod" ] ; then
    if [ "$2" == "-start" ] ; then
        softwares/mongodb-linux-x86_64-3.6.2/bin/mongod -dbpath data/db --logpath log/mongodb.log &
        echo -e "******************************************************************************************"
        echo -e "Starting mongod at PORT 27017, dbpath=/data/db, logpath=log/mongodb.log 64-bit"
        echo -e "******************************************************************************************"
    fi

    if [ "$2" == "-stop" ] ; then
        softwares/mongodb-linux-x86_64-3.6.2/bin/mongod --shutdown
        echo -e "******************************************************************************************"
        echo -e "Stopping Mongod"
        echo -e "******************************************************************************************"
    fi

    if [ "$2" == "-show" ] ; then
        ps -edaf | grep mongo | grep -v grep
    fi
fi


if [ "$1" == "-mongo" ] ; then
    if [ "$2" == "-start" ] ; then
        mongo
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
