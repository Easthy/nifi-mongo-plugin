### Description
This is NiFI plugin for reading mongo oplog. It uses tailable await cursor type. This cursor type keeps connection open, so processor started and keeps working without exiting. If you change cursor type to non-tailable, you must save checkpoints after each received event (modify saveCheckPoint function) as outputEvents function sets cursor to lastOplogTimestamp when invoked (it is invoked in onTrigger function, which in turn is invoked once in interval, set at processor settings).  
Due to using tailable await cursor type, processor is not able to be stopped just on clicking stop button as it awaits any new oplog entry to be received (stuck in onTrigger function).
  
Read processor's fields description when configuring it.  
  
![alt text](https://github.com/Easthy/nifi-mongo-plugin/blob/master/ProcessorScreenshot.png)


#### Format of message
```{"op":"i","changes":{"x":2,"y":0,"_id":"5d9c96a4735f1e9ff5493300","ts":1570543268},"collection":"events2","db":"test"}```  
op is event type (i == insert, d == delete, u == update)  

#### How-to
1. `mvn clean package`   
2. `cp nifi-compose-nar/target/nifi-compose-nar-1.9.2.nar /opt/nifi/lib`  
3. `sh /opt/nifi/bin/nifi.sh restart`  