mvn clean package eclipse:eclipse; 

mvn exec:java -Dexec.mainClass="com.hw.http.HttpClientBenchmark" -Dexec.args="https://androidnetworktester.googlecode.com/files/1mb.txt 1 1"

Arg 1: You can replace the URL with any file you would like to download.
Arg 2: Concurrency factor (10 means 10 threads)
Arg 3: Number of iterations.  If you specific 10 as concurrency and 10 as iterations, it will be executed 10x10 = 100 times

 