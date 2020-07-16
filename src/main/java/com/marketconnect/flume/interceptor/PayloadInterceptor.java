package com.marketconnect.flume.interceptor;

import com.google.common.base.Charsets;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import org.codehaus.janino.ExpressionEvaluator;

import java.lang.reflect.InvocationTargetException;

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

    private String[] variables;
    private String payload;
    private String output;
    private ExpressionEvaluator ee;

    public PayloadInterceptor(String variables, String payload, String output) {
        if (variables != null && variables.length() > 0)
            this.variables = variables.split(",");
        else
            this.variables = new String[0];
        this.payload = payload;
        this.output = output;
        logger.debug("Payload is " + payload + " with dest " + output);
    }

    @Override
    public void initialize() {
            List<String> parameterNames = new ArrayList<String>();
            List<Class<?>> parameterTypes = new ArrayList<Class<?>>();
            parameterNames.add( Constants.EVENT_BODY );
            parameterTypes.add( String.class );
            for (int i = 0; i < variables.length; i++) {
                parameterNames.add( variables[i] );
                parameterTypes.add( String.class );
            }
            ee = new ExpressionEvaluator();
            ee.setParameters(parameterNames.toArray( new String[parameterNames.size()] ), parameterTypes.toArray( new Class<?>[parameterTypes.size()] ) );
            ee.setReturnType( Object.class );
            ee.setThrownExceptions( new Class<?>[] { Exception.class } );
        try {
            ee.cook(payload);
        } catch (Exception e) {
            logger.warn("Initializing failed: unknown error. ", e);
        }
    }

    @Override
    public Event intercept(Event event) {
        try {

            Map<String, String> headers = event.getHeaders();
            List<String> argumentData = new ArrayList<String>();
            argumentData.add(new String(event.getBody(), Charsets.UTF_8));
            for (int i = 0; i < variables.length; i++) {
                argumentData.add(headers.get(variables[i]));
            }

            String body = null;
            try {
                Object result = ee.evaluate( argumentData.toArray( new String[argumentData.size()] ) );
                if (result != null) {
                    body = (String) result;
                }
            } catch (InvocationTargetException e) {
                logger.error(e + " event " + argumentData.toString() + " payload " + payload);
            }
            logger.debug("Result is " + body);

            if (body != null && body.length() > 0) {
                if (Constants.EVENT_BODY.equalsIgnoreCase(output)) {
                    event.setBody(body.getBytes(Charsets.UTF_8));
                } else {
                    headers.put(output, body);
                }
            } else if (Constants.EVENT_BODY.equalsIgnoreCase(output)) {
                event.setBody("".getBytes(Charsets.UTF_8));
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

        private String variables;
        private String payload;
        private String output;

        @Override
        public void configure(Context context) {
            variables = context.getString(Constants.CONFIG_VARIABLES);
            payload = context.getString(Constants.CONFIG_PAYLOAD);
            output = context.getString(Constants.CONFIG_OUTPUT, Constants.EVENT_BODY);
        }

        @Override
        public PayloadInterceptor build() {
            return new PayloadInterceptor(variables, payload, output);
        }

    }


    public static class Constants {

        public static final String CONFIG_VARIABLES = "variables";
        public static final String CONFIG_PAYLOAD = "payload";
        public static final String CONFIG_OUTPUT = "output";
        public static final String EVENT_BODY = "event_body";
    }
}
