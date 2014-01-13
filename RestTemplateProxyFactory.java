package it.sourcery.remote.executor;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Proxy;
import java.lang.reflect.Type;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.context.request.async.DeferredResult;
import org.springframework.web.util.UriComponentsBuilder;


public class RestTemplateProxyFactory {
	
	public <T> T proxy(final Class<T> interfaceClass, final String baseUri){
		return proxy(interfaceClass, baseUri, null, null);
	}
	
	public <T> T proxy(final Class<T> interfaceClass, final String baseUri, RestTemplate restTemplate){
		return proxy(interfaceClass, baseUri, restTemplate, null);
	}
	
	public <T> T proxy(final Class<T> interfaceClass, final String baseUri,  RestTemplate restTemplate, Executor asyncExecutor){
		return (T) Proxy.newProxyInstance(
				interfaceClass.getClassLoader(), 
				new Class[]{interfaceClass}, 
				new RestTemplateInvocationHandler<T>(interfaceClass, baseUri, restTemplate, asyncExecutor)
			);
	}
	
	private static ThreadFactory daemonThreadFactory() {
	    final ThreadFactory f = java.util.concurrent.Executors.defaultThreadFactory();
	    return new ThreadFactory() {
	        public Thread newThread(Runnable r) {
	            Thread t = f.newThread(r);
	            t.setDaemon(true);
	            return t;
	        }
	    };
	}
	
	class RestTemplateInvocationHandler<T> implements InvocationHandler {
	
		
		private Class<T> interfaceClass;
		private List<MediaType> mediaTypes = new ArrayList<MediaType>();
		private String baseUri;
		private RestTemplate restTemplate;
		private Executor asyncExecutor;
		
		private RestTemplateInvocationHandler(Class<T> interfaceClass, String baseUri){
			this(interfaceClass, baseUri, null, null);
		}
		
		private RestTemplateInvocationHandler(Class<T> interfaceClass, String baseUri, RestTemplate restTemplate, Executor asyncExecutor){
			this.interfaceClass = interfaceClass;
			this.baseUri = baseUri;
			this.restTemplate = restTemplate;
			if (this.restTemplate == null){
				this.restTemplate = new RestTemplate();
			}
			this.asyncExecutor = asyncExecutor;
			if (this.asyncExecutor == null){
				this.asyncExecutor = Executors.newCachedThreadPool(daemonThreadFactory());
			}			
			for(HttpMessageConverter<?> httpMessageConverter : this.restTemplate.getMessageConverters()){
				mediaTypes.addAll(httpMessageConverter.getSupportedMediaTypes());
			}
		}
	
		
		@Override
		public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
			
			UriComponentsBuilder uriComponentsBuilder = UriComponentsBuilder.fromUriString(baseUri);
			RequestMethod requestMethod = null;
			Map<String, String> pathParams = new HashMap<String, String>();
			Object messageBody = null;
			HttpHeaders headers = new HttpHeaders();
			List<MediaType> acceptableMediaTypes = new ArrayList<MediaType>();
			List<MediaType> consumesMediaTypes = new ArrayList<MediaType>();
			
			if (interfaceClass.isAnnotationPresent(RequestMapping.class)){
				RequestMapping requestMapping = interfaceClass.getAnnotation(RequestMapping.class);
				if (requestMapping.value() != null && requestMapping.value().length > 0){
					uriComponentsBuilder.path(requestMapping.value()[0]);
				}
				if (requestMapping.method() != null && requestMapping.method().length > 0) {
					requestMethod = requestMapping.method()[0];
				}
				if (requestMapping.consumes() != null && requestMapping.consumes().length > 0){
					consumesMediaTypes.clear();
					for (String stringMediaType : requestMapping.consumes()){
						MediaType mediaType = MediaType.parseMediaType(stringMediaType);
						consumesMediaTypes.add(mediaType);
					}
				}
				if (requestMapping.produces() != null && requestMapping.produces().length > 0){
					acceptableMediaTypes.clear();
					for (String stringMediaType : requestMapping.produces()){
						MediaType mediaType = MediaType.parseMediaType(stringMediaType);
						acceptableMediaTypes.add(mediaType);
					}
				}
				
			}
			
			if (method.isAnnotationPresent(RequestMapping.class)){
				RequestMapping requestMapping = method.getAnnotation(RequestMapping.class);
				if (requestMapping.value() != null && requestMapping.value().length > 0){
					uriComponentsBuilder.path(requestMapping.value()[0]);
				}
				if (requestMapping.method() != null && requestMapping.method().length > 0) {
					requestMethod = requestMapping.method()[0];
				}
				if (requestMapping.consumes() != null && requestMapping.consumes().length > 0){
					consumesMediaTypes.clear();
					for (String stringMediaType : requestMapping.consumes()){
						MediaType mediaType = MediaType.parseMediaType(stringMediaType);
						consumesMediaTypes.add(mediaType);
					}				
				}
				if (requestMapping.produces() != null && requestMapping.produces().length > 0){
					acceptableMediaTypes.clear();
					for (String stringMediaType : requestMapping.produces()){
						MediaType mediaType = MediaType.parseMediaType(stringMediaType);
						acceptableMediaTypes.add(mediaType);
					}
				}
			}
			
	
								
			int argumentId = 0;
			for(Annotation[] annotations : method.getParameterAnnotations()){
				for(Annotation annotation : annotations){
					if (annotation.annotationType().isAssignableFrom(PathVariable.class)){
						PathVariable pathVariable = (PathVariable)annotation;
						pathParams.put(pathVariable.value(), getArgumentToString(args, argumentId));
					}
					if (annotation.annotationType().isAssignableFrom(RequestParam.class)){
						RequestParam requestParam = (RequestParam)annotation;
						String value = getArgumentToString(args, argumentId);
						if (value == null){
							value = requestParam.defaultValue();
						} 
						if (value == null && requestParam.required()){
							throw new IllegalArgumentException("argument " + argumentId + " cannot be null");
						}
						if (value != null){
							uriComponentsBuilder.queryParam(requestParam.value(), value);							
						}
					}
					if (annotation.annotationType().isAssignableFrom(RequestHeader.class)){
						RequestHeader requestHeader = (RequestHeader)annotation;
						String value = getArgumentToString(args, argumentId);
						if (value == null){
							value = requestHeader.defaultValue();
						} 
						if (value == null && requestHeader.required()){
							throw new IllegalArgumentException("argument " + argumentId + " cannot be null");
						}
						if (value != null) {
							headers.set(requestHeader.value(), value);
						}
					}
					if (annotation.annotationType().isAssignableFrom(RequestBody.class)){
						RequestBody requestBody = (RequestBody)annotation;
						messageBody = args[argumentId];
	
					}
					
				}
				argumentId++;
			}	
	
				
			if (!acceptableMediaTypes.isEmpty()){
				headers.setAccept(acceptableMediaTypes);
			} else {
				headers.setAccept(mediaTypes);
			}
			
			HttpEntity<?> requestEntity;
			if (messageBody == null){
				requestEntity = new HttpEntity<Object>(headers);
			} else {
				requestEntity = new HttpEntity<Object>(messageBody, headers);
				if (!consumesMediaTypes.isEmpty()){
					for (MediaType mediaType : consumesMediaTypes){
						if (mediaTypes.contains(mediaType)){
							headers.setContentType(mediaType);
						}
					}
					if (headers.getContentType() == null){
						
						String message = "Could not write request: no suitable HttpMessageConverter found for request types [" +
								messageBody.getClass().getName() + "]";
						message += " and content types [" + consumesMediaTypes.toString() + "]";
						throw new RestClientException(message);
					}
				}
	
			}
			URI uri = uriComponentsBuilder.build().expand(pathParams).toUri();
			Class<?> returnType = method.getReturnType();
			boolean isAsync = false;
			if (method.getReturnType().isAssignableFrom(DeferredResult.class)){			
				returnType = Object.class;
				ParameterizedType type = (ParameterizedType) method.getGenericReturnType();
				Type[] types = type.getActualTypeArguments();
				if (types != null  && types.length > 0){
					returnType = (Class<?>)types[0];
				}
				isAsync = true;
			}
		
			if (isAsync){
				return async(uri, convert(requestMethod), requestEntity, returnType);
			} else {
				return sync(uri, convert(requestMethod), requestEntity, returnType);
			}
		}
	
		private String getArgumentToString(Object[] args, int argumentId) {
			Object obj = args[argumentId];
			if (obj == null) return null;
			return obj.toString();
		}
		
		private HttpMethod convert(RequestMethod method){
			if (method == null) return HttpMethod.GET;
			switch (method){
				case GET: return HttpMethod.GET;
				case DELETE: return HttpMethod.DELETE;
				case HEAD: return HttpMethod.HEAD;
				case OPTIONS: return HttpMethod.OPTIONS;
				case PATCH: return HttpMethod.PATCH;
				case POST: return HttpMethod.POST;
				case PUT: return HttpMethod.PUT;
				case TRACE: return HttpMethod.TRACE;
				default:
					return HttpMethod.GET;
			}
		}
		
		public <T> DeferredResult<T> async(final URI uri, final HttpMethod method, final HttpEntity<?> requestEntity, final Class<T> responseType){
			
			if (asyncExecutor instanceof ExecutorService){
				final FutureDeferredResult<T> deferredResult = new FutureDeferredResult<T>();
				deferredResult.setFuture(((ExecutorService)asyncExecutor).submit(new Callable<T>() {
					@Override
					public T call() throws Exception {
						try {
							T body = sync(uri, method, requestEntity, responseType);
							deferredResult.setResult(body);
							return body;
						} catch (RuntimeException t){
							deferredResult.setErrorResult(t);
							throw t;
						}				}
				}));
				return deferredResult;
			} else {
				final DeferredResult<T> deferredResult = new DeferredResult<T>();
				asyncExecutor.execute(new Runnable() {
					@Override
					public void run() {
						try {
							T body = sync(uri, method, requestEntity, responseType);
							deferredResult.setResult(body);
						} catch (RuntimeException t){
							deferredResult.setErrorResult(t);
						}				
					}
				});
				return deferredResult;
			}		
		}
		
		public <T> T sync(URI uri, HttpMethod method, HttpEntity<?> requestEntity, Class<T> responseType){
			return restTemplate.exchange(uri, method, requestEntity, responseType).getBody();
		}
		
		
	}
	
	
	class FutureDeferredResult<T> extends DeferredResult<T> implements Future<T> {
		
		private Future<T> future;
		private T response;
		private Throwable throwable;
		
		public void setFuture(Future<T> future){
			this.future = future;
		}
		
		public boolean setResult(T result) {
			this.response = result;
			return super.setResult(result);
		}
		public boolean setErrorResult(Throwable result) {
			this.throwable = result;
			return super.setErrorResult(result);
		}
		
		/**
		 * @param mayInterruptIfRunning
		 * @return
		 * @see java.util.concurrent.Future#cancel(boolean)
		 */
		public boolean cancel(boolean mayInterruptIfRunning) {
			if (future != null){
				return future.cancel(mayInterruptIfRunning);
			} else {
				return false;
			}
		}
	
		/**
		 * @return
		 * @see java.util.concurrent.Future#isCancelled()
		 */
		public boolean isCancelled() {
			if (future != null){
				return future.isCancelled();
			} else {
				return false;
			}
		}
	
		/**
		 * @return
		 * @see java.util.concurrent.Future#isDone()
		 */
		public boolean isDone() {
			if(response != null || throwable != null){
				return true;
			} else if (future != null){
				return future.isDone();
			} else {
				return false;
			}
		}
	
		/**
		 * @return
		 * @throws InterruptedException
		 * @throws ExecutionException
		 * @see java.util.concurrent.Future#get()
		 */
		public T get() throws InterruptedException, ExecutionException {
			if(response != null){
				return response;
			} else if (throwable != null){
				throw new ExecutionException(throwable);
			} else if (future != null){
				return future.get();
			} else {
				throw new ExecutionException(new IllegalStateException("You need to execute via async() or sync() call first"));
			}
		}
	
		/**
		 * @param timeout
		 * @param unit
		 * @return
		 * @throws InterruptedException
		 * @throws ExecutionException
		 * @throws TimeoutException
		 * @see java.util.concurrent.Future#get(long, java.util.concurrent.TimeUnit)
		 */
		public T get(long timeout, TimeUnit unit) throws InterruptedException,
				ExecutionException, TimeoutException {
			if(response != null){
				return response;
			} else if (throwable != null){
				throw new ExecutionException(throwable);
			} else if (future != null){
				return future.get(timeout, unit);
			} else {
				throw new ExecutionException(new IllegalStateException("You need to execute via async() or sync() call first"));
			}
		}
	}
}
