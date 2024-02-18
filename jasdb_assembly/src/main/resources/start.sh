#!/bin/sh

if [ ! -e "${JAVA_HOME}" ]; then
  echo "Unable to start JASDB, JAVA_HOME must be set"
  exit 1
fi

for i in `ls /jasdb/lib/*.jar`
do
  DB_CLASSPATH=${DB_CLASSPATH}:${i}
done

DB_CLASSPATH=${DB_CLASSPATH}:/jasdb/configuration/
echo "$DB_CLASSPATH"

${JAVA_HOME}/bin/java -cp ".:${DB_CLASSPATH}" -Dcom.sun.management.jmxremote.port=9999 -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -Xmx1024m com.oberasoftware.jasdb.service.JasDBMain

