package com.hw.http;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.hadoop.io.BoundedByteArrayOutputStream;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;

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

/**
 * Intention is to understand the performance of various
 * Http Clients available (HttpUrlConnection, Netty, Apache commons,
 * Apache AsyncClients etc)
 *
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

	// for old common-httpclient
	MultiThreadedHttpConnectionManager connectionManager = new MultiThreadedHttpConnectionManager();
	HttpClient old_http_common_client = new HttpClient(connectionManager);

	// For latest http 4.x
	PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
	CloseableHttpClient http_4x_client = HttpClients.custom()
			.setConnectionManager(cm).build();

	// For http async
	CloseableHttpAsyncClient asyncHttpClient = HttpAsyncClients.createDefault();

	interface Benchmark extends Callable {
		void execute() throws IOException;
	}

	abstract class AbstractBenchmark implements Benchmark {
		String url;
		boolean checksum;

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
	}

	/**
	 * initialize the connection managers
	 */
	public void init() {
		asyncHttpClient.start();
	}

	/**
	 * Cleanup the connection managers
	 */
	public void cleanup() {
		try {
			connectionManager.shutdownAll();
			cm.shutdown();
			asyncHttpClient.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * 
	 * Based on Http AsyncHttpClient
	 *
	 */
	class AsyncHttpClientBenchmark extends AbstractBenchmark {

		final CountDownLatch latch = new CountDownLatch(1);
		
		public AsyncHttpClientBenchmark(String url) {
			super(url);
		}

		@Override
		public void execute() throws IOException {
			long sTime = System.currentTimeMillis();

			try {
				HttpGet request = new HttpGet(url);
				
				Future<HttpResponse> future = asyncHttpClient.execute(request,
						new FutureCallback<HttpResponse>() {

							@Override
							public void cancelled() {
								// ignore
							}

							@Override
							public void completed(HttpResponse response) {
								try {
									int length = Integer.parseInt(response.getFirstHeader(
											"Content-Length").getValue());
									int read = readData(new BufferedInputStream(response.getEntity()
											.getContent(), 8192), length);
									System.out.println("AsyncClient : " + read);
								} catch (IllegalStateException e) {
									e.printStackTrace();
								} catch (IOException e) {
									e.printStackTrace();
								}
								latch.countDown();
							}

							@Override
							public void failed(Exception exp) {
								// Ignore
							}
						});
			} catch (Throwable t) {
				t.printStackTrace();
			}
			try {
				latch.await();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			long eTime = System.currentTimeMillis();
			asynchHttpClientHistogram.update(eTime - sTime);
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
		public void execute() throws IOException {
			long sTime = System.currentTimeMillis();
			URL oracle = new URL(url);
			HttpURLConnection connection = (HttpURLConnection) oracle
					.openConnection();
			connection.connect();

			int length = Integer
					.parseInt(connection.getHeaderField("Content-Length"));
			int read = readData(new BufferedInputStream(connection.getInputStream(), 8192),
					length);
			long eTime = System.currentTimeMillis();
			System.out.println("StdURLConnection : " + read);
			urlConnectionHistorgram.update(eTime - sTime);

		}
	}

	/**
	 * Based on old apache commons client.  Its a blocking call
	 *
	 */
	class CommonsHttpBenchmark extends AbstractBenchmark {

		public CommonsHttpBenchmark(String url) {
			super(url);
		}

		@Override
		public void execute() throws IOException {
			long sTime = System.currentTimeMillis();

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
	}

	
	/**
	 * 
	 * Based on Netty async client
	 *
	 */
	class NettyBenchmark extends AbstractBenchmark {

		public NettyBenchmark(String url) {
			super(url);
		}

		@Override
		public void execute() throws IOException {
			long sTime = System.currentTimeMillis();
			Builder builder = new AsyncHttpClientConfig.Builder();
			builder.setCompressionEnabled(false).setAllowPoolingConnection(true)
					.setRequestTimeoutInMs(30000).build();
			AsyncHttpClient client = new AsyncHttpClient(builder.build());

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
				Response response = client.prepareGet(url).execute(asyncHandler).get();
				int read = readData(new BufferedInputStream(response.getResponseBodyAsStream(),
						8192), Integer.parseInt(response.getHeader("Content-Length")));
				System.out.println("NettyAsyncClient : " + read);
			} catch (InterruptedException e) {
				throw new IOException(e);
			} catch (ExecutionException e) {
				throw new IOException(e);
			} finally {
				client.close();
			}

			long eTime = System.currentTimeMillis();
			nettyHistogram.update(eTime - sTime);
		}
	}

	/**
	 * 
	 * Based on Http 4.x components framework
	 *
	 */
	class LatestHttpComponentsBenchmark extends AbstractBenchmark {
		public LatestHttpComponentsBenchmark(String url) {
			super(url);
		}

		@Override
		public void execute() throws IOException {
			long sTime = System.currentTimeMillis();
			CloseableHttpClient httpclient = HttpClients.createDefault();
			try {
				HttpGet httpget = new HttpGet(url);

				System.out.println("Executing request " + httpget.getRequestLine());

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
				String responseBody = httpclient.execute(httpget, responseHandler);
			} finally {
				httpclient.close();
			}
			long eTime = System.currentTimeMillis();
			latestHttpComponentsHistogram.update(eTime - sTime);

		}
	}

	/**
	 * Allocate in memory buffers and download the data
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
	 * @param size
	 * @return
	 */
	static byte[] getMemory(int size) {
		BoundedByteArrayOutputStream byteStream = new BoundedByteArrayOutputStream(
				size);
		byte[] memory = byteStream.getBuffer();
		return memory;
	}

	/**
	 * Read the entire payload
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
	 * @param url
	 */
	public HttpClientBenchmark(String url) {
		this.url = url;
		init();
		reporter.start(10, TimeUnit.SECONDS);
	}

	/**
	 * Execute a specific benchmark with concurrent threads
	 * @param b benchmark to be executed
	 * @param concurrency
	 * @throws IOException
	 */
	public void benchmark(Benchmark b, int concurrency) throws IOException {
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
	}

	/**
	 * Main method
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

		test.cleanup();

		reporter.report(metrics.getGauges(), metrics.getCounters(),
				metrics.getHistograms(), metrics.getMeters(), metrics.getTimers());
		reporter.stop();
		System.exit(-1);
	}
}

