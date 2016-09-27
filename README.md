WSDM Cup 2017 Data Server
====================
This program provides evaluation data to participants of the WSDM Cup 2017 and ensures that participants report vandalism scores in near real time.

The program is conceptually based upon three binary streams send over the network:

  1. One stream provides uncompressed Wikidata revisions (in the same format as in the wdvc16_YYYY_MM.xml files)
  2. One stream provides uncompressed meta data (in the same format as in the wdvc16_meta.csv files)
  3. One stream receives the vandalism scores (as specified in the output format on the WSDM Cup website)

All three byte streams are send over a single TCP connection. The simple protocol is as follows:

  1. The client software connects to the server, sends a given authentication token, and terminates the line with '\r\n'
  2. The server sends revisions and meta data in a multiplexed way to the client
	1. number of meta bytes to be send (encoded as int32 in network byte order)
	2. meta bytes
	3. number of revision bytes to be send (encoded as int32 in network byte order)
	4. revision bytes
  3. The server closes the output socket as soon as there is no more data to send (half close)
  4. The client closes the output socket as soon as there are no more scores to send
  	
To ensure that participants report vandalism scores in a timely manner, the server provides new data as soon as the client reports vandalism scores for previous revisions.
More precisely, we introduce a backpressure window of k revisions, i.e., the client receives data for revision n + k as soon as having reported the vandalism score for revision n (the exact constant k is still to be determined but you can expect it to be around 16 revisions).


Installation
------------
In Eclipse, executing "Run As -> Maven install" creates a JAR file which includes all dependencies.


Execution
--------------------
For every participant, a separate server instance on a different host/port combination is started. The command line arguments are  
  -r Path to compressed revision file  
  -m Path to compressed meta file  
  -o Path to output directory  
  -p Port the server is listening on
 

Connecting to the server
------------------------
You can find a demo programs how to connect to the server on the WSDM Cup GitHub page.
