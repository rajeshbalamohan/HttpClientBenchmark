package com.hw.http;

import com.google.common.base.Preconditions;
import com.ning.http.client.AsyncHandler;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.AsyncHttpClientConfig;
import com.ning.http.client.AsyncHttpClientConfig.Builder;
import com.ning.http.client.HttpResponseBodyPart;
import com.ning.http.client.HttpResponseHeaders;
import com.ning.http.client.HttpResponseStatus;
import com.ning.http.client.Response;
import com.yammer.metrics.ConsoleReporter;
import com.yammer.metrics.Histogram;
import com.yammer.metrics.MetricRegistry;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.HttpVersion;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.hadoop.io.BoundedByteArrayOutputStream;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.DefaultConnectionReuseStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.nio.reactor.ConnectingIOReactor;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Intention is to understand the performance of various
 * Http Clients available (HttpUrlConnection, Netty, Apache commons,
 * Apache AsyncClients etc)
 */
public class HttpClientBenchmark {

  //Metrics registry
  private static final MetricRegistry metrics = new MetricRegistry("Test_1");

  static final Histogram latestHttpComponentsHistogram = metrics
      .histogram("http-components-4.x");
  static final Histogram asynchHttpClientHistogram = metrics
      .histogram("async-client");
  static final Histogram urlConnectionHistorgram = metrics
      .histogram("url-connection");
  static final Histogram commonsClientHistogram = metrics
      .histogram("commons-client");
  static final Histogram nettyHistogram = metrics.histogram("netty-client");

  static final ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics)
      .build();

  private String url;

  static boolean HTTP_1_0 = false;

  interface Benchmark extends Callable {
    void init() throws Exception;

    void execute() throws IOException;

    void cleanup();
  }

  abstract class AbstractBenchmark implements Benchmark {
    String url;

    public AbstractBenchmark(String url) {
      this.url = url;
    }

    @Override
    public Object call() throws Exception {
      try {
        this.execute();
      } catch (Throwable t) {
        t.printStackTrace();
      }
      return null;
    }

    @Override
    public void cleanup() {
    }
  }

  /**
   * Based on Http AsyncHttpClient
   */
  class AsyncHttpClientBenchmark extends AbstractBenchmark {
    // For http async
    CloseableHttpAsyncClient asyncHttpClient;
    PoolingNHttpClientConnectionManager asyncMgr;

    public AsyncHttpClientBenchmark(String url) {
      super(url);
    }

    @Override
    public void cleanup() {
      try {
        asyncHttpClient.close();
        asyncMgr.shutdown();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    @Override public void init() throws Exception {

      RequestConfig requestConfig = RequestConfig
          .custom()
          .setConnectionRequestTimeout(3 * 60 * 1000)
          .setSocketTimeout(3 * 60 * 1000)
          .setStaleConnectionCheckEnabled(true)
          .build();

      IOReactorConfig ioReactorConfig = IOReactorConfig
          .custom()
          .setIoThreadCount(20)
          .setConnectTimeout(3 * 60 * 1000)
          .build();

      ConnectingIOReactor ioReactor = new DefaultConnectingIOReactor(ioReactorConfig);

      asyncMgr = new PoolingNHttpClientConnectionManager
          (ioReactor);
      asyncMgr.setMaxTotal(30);
      asyncMgr.setDefaultMaxPerRoute(5);

      Preconditions.checkState(asyncMgr != null, "Async PoolManager has to be available");
      HttpAsyncClientBuilder builder = HttpAsyncClients
          .custom()
          .setConnectionReuseStrategy(new DefaultConnectionReuseStrategy())
          .setConnectionManager(asyncMgr)
          .setDefaultRequestConfig(requestConfig);

      asyncHttpClient = builder.build();
      asyncHttpClient.start();

    }

    @Override
    public void execute() throws IOException {
      final long sTime = System.currentTimeMillis();

      try {
        HttpGet request = new HttpGet(url);

        if (HTTP_1_0) {
          System.out.println("Using 1.0 for async http client");
          request.setHeader("http.protocol.version", "HTTP/1.0");
        }

        Future<HttpResponse> future = asyncHttpClient.execute(request, null);
        try {
          HttpResponse response = future.get();
          int length = Integer.parseInt(response.getFirstHeader(
              "Content-Length").getValue());
          int read = readData(new BufferedInputStream(response.getEntity()
              .getContent(), 8192), length);
          long eTime = System.currentTimeMillis();
          asynchHttpClientHistogram.update(eTime - sTime);
        } catch (InterruptedException e) {
          e.printStackTrace();
        } catch (ExecutionException e) {
          e.printStackTrace();
        }
      } catch (Throwable t) {
        t.printStackTrace();
      }
    }
  }

  /**
   * Good old HttpURLConnection
   */
  class StandardURLConnectionBenchmark extends AbstractBenchmark {

    public StandardURLConnectionBenchmark(String url) {
      super(url);
    }

    @Override
    public void init() throws Exception {
      //Nothing.
    }

    @Override
    public void execute() throws IOException {
      long sTime = System.currentTimeMillis();
      URL oracle = new URL(url);
      HttpURLConnection connection = (HttpURLConnection) oracle.openConnection();
      connection.connect();

      int length = Integer.parseInt(connection.getHeaderField("Content-Length"));
      int read = readData(new BufferedInputStream(connection.getInputStream(), 8192), length);
      long eTime = System.currentTimeMillis();
      System.out.println("StdURLConnection : " + read);
      urlConnectionHistorgram.update(eTime - sTime);
    }
  }

  /**
   * Based on old apache commons client.  Its a blocking call
   */
  class CommonsHttpBenchmark extends AbstractBenchmark {

    // for old common-httpclient
    MultiThreadedHttpConnectionManager connectionManager = new
        MultiThreadedHttpConnectionManager();
    HttpClient old_http_common_client;


    public CommonsHttpBenchmark(String url) {
      super(url);
    }

    @Override
    public void init() throws Exception {
      // for old common-httpclient
      connectionManager.setMaxConnectionsPerHost(20);
      connectionManager.setMaxTotalConnections(20);
      old_http_common_client = new HttpClient(connectionManager);
    }

    @Override
    public void execute() throws IOException {
      long sTime = System.currentTimeMillis();
      if (HTTP_1_0) {
        System.out.println("Using 1.0 for old http client");
        old_http_common_client.getParams().setVersion(new HttpVersion(1, 0));
      }
      GetMethod method = new GetMethod(url);
      int statusCode = old_http_common_client.executeMethod(method);

      if (statusCode != HttpStatus.SC_OK) {
        System.err.println("Method failed: " + method.getStatusLine());
      }

      int length = Integer.parseInt(method.getResponseHeader("Content-Length")
          .getValue());
      int read = readData(new BufferedInputStream(method.getResponseBodyAsStream(), 8192),
          length);
      long eTime = System.currentTimeMillis();
      System.out.println("Common3xClient : " + read);
      commonsClientHistogram.update(eTime - sTime);

      method.releaseConnection();
    }

    @Override
    public void cleanup() {
      connectionManager.shutdown();
    }
  }

  /**
   * Based on Netty async client
   */
  class NettyBenchmark extends AbstractBenchmark {
    //For Netty
    AsyncHttpClient nettyClient;

    public NettyBenchmark(String url) {
      super(url);
    }

    @Override
    public void init() throws Exception {
      //Netty
      Builder builder = new AsyncHttpClientConfig.Builder();
      builder.setAllowPoolingConnection(true)
          .setCompressionEnabled(false).setMaximumConnectionsTotal(20).setMaximumConnectionsPerHost(20).build();
      nettyClient = new AsyncHttpClient(builder.build());
    }

    @Override
    public void execute() throws IOException {
      long sTime = System.currentTimeMillis();
      AsyncHandler<Response> asyncHandler = new AsyncHandler<Response>() {
        private final Response.ResponseBuilder builder = new Response.ResponseBuilder();

        public STATE onBodyPartReceived(final HttpResponseBodyPart content)
            throws Exception {
          builder.accumulate(content);
          return STATE.CONTINUE;
        }

        public STATE onStatusReceived(final HttpResponseStatus status)
            throws Exception {
          builder.accumulate(status);
          return STATE.CONTINUE;
        }

        public STATE onHeadersReceived(final HttpResponseHeaders headers)
            throws Exception {
          builder.accumulate(headers);
          return STATE.CONTINUE;
        }

        public Response onCompleted() throws Exception {
          return builder.build();
        }

        @Override
        public void onThrowable(Throwable t) {
          // TODO Auto-generated method stub
          t.printStackTrace();
        }
      };

      try {
        Response response = nettyClient.prepareGet(url).execute(asyncHandler).get();
        int read = readData(new BufferedInputStream(response.getResponseBodyAsStream(),
            8192), Integer.parseInt(response.getHeader("Content-Length")));
        System.out.println("NettyAsyncClient : " + read);
      } catch (InterruptedException e) {
        throw new IOException(e);
      } catch (ExecutionException e) {
        throw new IOException(e);
      }

      long eTime = System.currentTimeMillis();
      nettyHistogram.update(eTime - sTime);
    }

    @Override
    public void cleanup() {
      nettyClient.close();
    }
  }

  /**
   * Based on Http 4.x components framework
   */
  class LatestHttpComponentsBenchmark extends AbstractBenchmark {
    //For LatestHttpComponentsBenchmark
    PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
    CloseableHttpClient latestHttpComponentsClient;


    public LatestHttpComponentsBenchmark(String url) {
      super(url);
    }

    @Override public void init() throws Exception {
      // For latest http 4.x
      cm.setDefaultMaxPerRoute(20);
      cm.setMaxTotal(20);
      latestHttpComponentsClient = HttpClients.custom()
          .setConnectionManager(cm).build();
    }

    @Override
    public void execute() throws IOException {
      long sTime = System.currentTimeMillis();
      try {
        HttpGet httpget = new HttpGet(url);
        if (HTTP_1_0) {
          System.out.println("Using 1.0 for latestHtpComponents Benchmark");
          httpget.addHeader("http.protocol.version", "HTTP/1.0");
        }

        //System.out.println("Executing request " + httpget.getRequestLine());

        // Create a custom response handler
        ResponseHandler<String> responseHandler = new ResponseHandler<String>() {

          public String handleResponse(final HttpResponse response)
              throws ClientProtocolException, IOException {
            int status = response.getStatusLine().getStatusCode();
            if (status >= 200 && status < 300) {
              int length = Integer.parseInt(response.getFirstHeader(
                  "Content-Length").getValue());
              int read = readData(response.getEntity().getContent(), (int) response
                  .getEntity().getContentLength());
              System.out.println("ApacheHttp_4.x : " + read);
              return "done";
            } else {
              throw new ClientProtocolException("Unexpected response status: "
                  + status);
            }
          }

        };
        String responseBody = latestHttpComponentsClient.execute(httpget, responseHandler);
      } catch (Throwable t) {
        t.printStackTrace();
      }
      long eTime = System.currentTimeMillis();
      latestHttpComponentsHistogram.update(eTime - sTime);
    }

    @Override
    public void cleanup() {
      try {
        latestHttpComponentsClient.close();
        cm.shutdown();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * Allocate in memory buffers and download the data
   *
   * @param input
   * @param length
   * @return
   * @throws IOException
   */
  static int readData(InputStream input, int length) throws IOException {
    // Copy map-output into an in-memory buffer
    byte[] shuffleData = getMemory(length);
    try {
      return readFully(input, shuffleData, 0, shuffleData.length);
    } catch (IOException ioe) {
      throw ioe;
    } finally {
      input.close();
    }
  }

  /**
   * Allocate memory to hold the data in memory
   *
   * @param size
   * @return
   */
  static byte[] getMemory(int size) {
    BoundedByteArrayOutputStream byteStream = new BoundedByteArrayOutputStream(size);
    byte[] memory = byteStream.getBuffer();
    return memory;
  }

  /**
   * Read the entire payload
   *
   * @param in
   * @param buf
   * @param off
   * @param len
   * @return
   * @throws IOException
   */
  static int readFully(InputStream in, byte buf[], int off, int len)
      throws IOException {
    int toRead = len;
    while (toRead > 0) {
      int ret = in.read(buf, off, toRead);
      if (ret < 0) {
        throw new IOException("Premature EOF from inputStream");
      }
      toRead -= ret;
      off += ret;
    }
    return (len - toRead);
  }

  /**
   * Constructor
   *
   * @param url
   */
  public HttpClientBenchmark(String url) {
    this.url = url;
    reporter.start(10, TimeUnit.SECONDS);
  }

  /**
   * Execute a specific benchmark with concurrent threads
   *
   * @param b           benchmark to be executed
   * @param concurrency
   * @throws IOException
   */
  public void benchmark(Benchmark b, int concurrency) throws Exception {
    long sInit = System.currentTimeMillis();
    b.init();
    long eInit = System.currentTimeMillis();

    long start = System.currentTimeMillis();
    ExecutorService service = Executors.newFixedThreadPool(50);
    for (int i = 0; i < concurrency; i++) {
      service.submit(b);
    }
    try {
      service.shutdown();
      service.awaitTermination(10000, TimeUnit.HOURS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    long end = System.currentTimeMillis();
    System.out.println("******** Total time taken for " + b.getClass() + "=" + (end - start) + " "
        + "ms, initTime=" + (eInit - sInit));
    b.cleanup();
  }

  /**
   * Main method
   *
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    if (args.length != 3) {
      System.out.println("Usage: java -cp blah HttpClientBenchmark <DOWNLOAD_FILE_URL> "
          + "<CONCURRENCY> <ITERATIONS>");
      System.out
          .println("Ex: java -server -Xmx4g  -cp "
              + ".:./target/*:"
              + "Test https://androidnetworktester.googlecode.com/files/1mb.txt 10 20");
      System.exit(-1);
    }

    HTTP_1_0 = Boolean.parseBoolean(System.getProperty("useHttp_1_0", "false"));

    String url = args[0];
    int concurrency = Integer.parseInt(args[1]);
    int iterations = Integer.parseInt(args[2]);

    HttpClientBenchmark test = new HttpClientBenchmark(url);

    //try to run all the tests for certain iterations
    for (int i = 0; i < iterations; i++) {
      test.benchmark(test.new CommonsHttpBenchmark(url), concurrency);
      test.benchmark(test.new AsyncHttpClientBenchmark(url), concurrency);
      test.benchmark(test.new LatestHttpComponentsBenchmark(url), concurrency);
      test.benchmark(test.new StandardURLConnectionBenchmark(url), concurrency);
      test.benchmark(test.new NettyBenchmark(url), concurrency);
    }

    reporter.report(metrics.getGauges(), metrics.getCounters(),
        metrics.getHistograms(), metrics.getMeters(), metrics.getTimers());
    reporter.stop();
    System.exit(-1);
  }
}

