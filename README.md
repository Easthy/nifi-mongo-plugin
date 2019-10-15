This is NiFI plugin for reading mongo oplog. It uses tailable await cursor type. This cursor type keeps connection open, so processor started and keeps working without exiting. If you change cursor type to non-tailable, you must save checkpoints after each received event as outputEvents function sets cursor to lastOplogTimestamp when invoked (it is invoked in onTrigger function, which in turn is invoked once in set at processor settings interval)

#### How-to
1. `mvn clean package`   
2. `cp nifi-compose-nar/target/nifi-compose-nar-1.9.2.nar /opt/nifi/lib`  
3. `sh /opt/nifi/bin/nifi.sh restart`  