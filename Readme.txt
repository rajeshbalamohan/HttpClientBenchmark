mvn clean package eclipse:eclipse; 

mvn exec:java -Dexec.mainClass="com.hw.http.HttpClientBenchmark" -Dexec.args="https://androidnetworktester.googlecode.com/files/1mb.txt 1 1"

Arg 1: You can replace the URL with any file you would like to download.
Arg 2: Concurrency factor (10 means 10 threads)
Arg 3: Number of iterations.  If you specific 10 as concurrency and 10 as iterations, it will be executed 10x10 = 100 times

Or you can run via normal java command
java -server -Xmx4g  -cp ./:<respective_jars> com.hw.http.HttpClientBenchmark https://androidnetworktester.googlecode.com/files/1mb.txt 20 10


Note that results will be different based on the network connecition in your environment.

Results (one of the hardware in the lab environment):
=====================================================

Async client is very impressive (mean is just 194 ms to download 1 MB of data with 20 concurrent connections).
However, plain HttpUrlConnection took  1722.43 ms

-- Histograms ------------------------------------------------------------------
async-client
             count = 200
               min = 70
               max = 465
              mean = 194.99
            stddev = 115.73
            median = 160.50
              75% <= 281.00
              95% <= 463.00
              98% <= 464.00
              99% <= 464.00
            99.9% <= 465.00
commons-client
             count = 200
               min = 74
               max = 2007
              mean = 850.75
            stddev = 557.76
            median = 789.00
              75% <= 1286.25
              95% <= 1796.85
              98% <= 1895.34
              99% <= 1945.67
            99.9% <= 2007.00
http-components-4.x
             count = 200
               min = 814
               max = 3878
              mean = 2175.72
            stddev = 571.00
            median = 2172.50
              75% <= 2582.00
              95% <= 3068.40
              98% <= 3513.74
              99% <= 3804.93
            99.9% <= 3878.00
netty-client
             count = 200
               min = 727
               max = 2717
              mean = 1609.76
            stddev = 534.68
            median = 1654.00
              75% <= 2028.50
              95% <= 2534.20
              98% <= 2688.58
              99% <= 2715.92
            99.9% <= 2717.00
url-connection
             count = 200
               min = 539
               max = 3164
              mean = 1722.43
            stddev = 606.41
            median = 1726.00
              75% <= 2151.50
              95% <= 2810.90
              98% <= 2971.84
              99% <= 3051.27
            99.9% <= 3164.00

 