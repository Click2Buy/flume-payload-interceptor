package com.marketconnect.flume.interceptor;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.marketconnect.flume.interceptor.PayloadInterceptor.Constants.CONFIG_PAYLOAD;

public class PayloadInterceptor implements Interceptor {
    private static final Logger logger =
            LoggerFactory.getLogger(PayloadInterceptor.class);

    private String payload;

    public PayloadInterceptor(String payload) {
        this.payload = payload;
        logger.debug("Payload is " + payload);
    }

    @Override
    public void initialize() {
    }

    @Override
    public Event intercept(Event event) {
        try {

            Map<String, String> headers = event.getHeaders();
            StringTokenizer stringTokenizer = new StringTokenizer(payload, " + ");
            StringBuffer body = new StringBuffer("");
            while (stringTokenizer.hasMoreElements()) {
                String token = stringTokenizer.nextToken();
                if ((token.startsWith("\"")) && (token.endsWith("\""))) {
                    body.append(token.substring(1, token.length() - 1));
                } else if ((token.startsWith("[")) && (token.endsWith("]"))) {
                    String value = headers.get(token.substring(1, token.length() - 1));
                    if (value != null) {
                        body.append(value);
                    }
                }
            }
            if (body.length() > 0) {
                event.setBody(body.toString().getBytes());
            } else {
                event.setBody("".getBytes());
            }

        } catch (java.lang.ClassCastException e) {
            logger.warn("Skipping event due to: ClassCastException.", e);
        } catch (Exception e) {
            logger.warn("Skipping event due to: unknown error.", e);
            e.printStackTrace();
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

        @Override
        public void configure(Context context) {
            payload = context.getString(CONFIG_PAYLOAD);
        }

        @Override
        public PayloadInterceptor build() {
            return new PayloadInterceptor(payload);
        }

    }


    public static class Constants {

        public static final String CONFIG_PAYLOAD = "payload";
    }
}
