package com.marketconnect.flume.interceptor;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import org.codehaus.janino.ExpressionEvaluator;

import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.marketconnect.flume.interceptor.PayloadInterceptor.Constants.CONFIG_PAYLOAD;

public class PayloadInterceptor implements Interceptor {
    private static final Logger logger =
            LoggerFactory.getLogger(PayloadInterceptor.class);

    private String payload;
    private String output;

    public PayloadInterceptor(String payload, String output) {
        this.payload = payload;
        this.output = output;
        logger.debug("Payload is " + payload + " with dest " + output);
    }

    @Override
    public void initialize() {
    }

    @Override
    public Event intercept(Event event) {
        try {

            Map<String, String> headers = event.getHeaders();
            final Iterator<String> headerIterator = headers.keySet().iterator();
            List<String> parameterNames = new ArrayList<String>();
            List<Class<?>> parameterTypes = new ArrayList<Class<?>>();
            List<String> argumentData = new ArrayList<String>();
            while (headerIterator.hasNext()) {
                final String currentHeader = headerIterator.next();
                parameterNames.add( currentHeader );
                parameterTypes.add( String.class );
                argumentData.add(headers.get(currentHeader));
            }

            ExpressionEvaluator ee = new ExpressionEvaluator();
            ee.setParameters(parameterNames.toArray( new String[parameterNames.size()] ), parameterTypes.toArray( new Class<?>[parameterTypes.size()] ) );
            ee.setReturnType( Object.class );
            ee.setThrownExceptions( new Class<?>[] { Exception.class } );
            ee.cook(payload);
            Object result = ee.evaluate( argumentData.toArray( new String[argumentData.size()] ) );
            String body = null;
            if (result != null) {
                body = (String) result;
            }
            logger.debug("Result is " + body);

            if (body.length() > 0) {
                if (Constants.DEFAULT_OUTPUT.equalsIgnoreCase(output)) {
                    event.setBody(body.getBytes());
                } else {
                    headers.put(output, body);
                }
            } else if (Constants.DEFAULT_OUTPUT.equalsIgnoreCase(output)) {
                event.setBody("".getBytes());
            }

        } catch (java.lang.ClassCastException e) {
            logger.warn("Skipping event due to: ClassCastException.", e);
        } catch (Exception e) {
            logger.warn("Skipping event due to: unknown error. " + event.toString(), e);
        }
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> events) {

        List<Event> interceptedEvents = new ArrayList<Event>(events.size());
        for (Event event : events) {
            Event interceptedEvent = intercept(event);
            interceptedEvents.add(interceptedEvent);
        }

        return interceptedEvents;
    }

    @Override
    public void close() {
    }

    public static class Builder implements Interceptor.Builder {

        private String payload;
        private String output;

        @Override
        public void configure(Context context) {
            payload = context.getString(Constants.CONFIG_PAYLOAD);
            output = context.getString(Constants.CONFIG_OUTPUT, Constants.DEFAULT_OUTPUT);
        }

        @Override
        public PayloadInterceptor build() {
            return new PayloadInterceptor(payload, output);
        }

    }


    public static class Constants {

        public static final String CONFIG_PAYLOAD = "payload";
        public static final String CONFIG_OUTPUT = "output";
        public static final String DEFAULT_OUTPUT = "event_body";
    }
}
