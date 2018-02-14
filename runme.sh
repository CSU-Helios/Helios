#!/bin/bash

if [ "$1" == "-mongod" ] ; then
        if [ "$2" == "-start" ] ; then
                /s/chopin/f/proj/helios/mongodb-linux-x86_64-3.6.2/bin/mongod -dbpath /s/chopin/f/proj/helios/data/db --logpath /s/chopin/f/proj/helios/log/mongodb.log &
                echo -e "******************************************************************************************\nStarting mongod at PORT 27017, dbpath=/data/db, logpath=log/mongodb.log 64-bit\n******************************************************************************************" &
        fi

        if [ "$2" == "-stop" ] ; then
                /s/chopin/f/proj/helios/mongodb-linux-x86_64-3.6.2/bin/mongod --shutdown
                echo -e "******************************************************************************************\nStopping Mongod\n******************************************************************************************"
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
                /s/chopin/f/proj/helios/softwares/Python-3.6.4/python   
        fi
	
        if [ "$2" == "-Helios" ] ; then
                if [ -z "$3" ]; then
                        /s/chopin/f/proj/helios/softwares/Python-3.6.4/python Helios.py -h
                else
                        /s/chopin/f/proj/helios/softwares/Python-3.6.4/python Helios.py $3 $4 $5 $6 $7 $8 $9 $10 $11 $12 $13 $14 $15        
                fi
        fi

fi
