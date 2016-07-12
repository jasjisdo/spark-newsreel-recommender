package de.dailab.newsreel.recommender.metarecommender.delegate;

import de.dailab.newsreel.recommender.common.extimpl.PlistaItem;
import de.dailab.newsreel.recommender.common.util.EvalLevel;
import de.dailab.newsreel.recommender.metarecommender.inter.RequestHandler;
import de.dailab.newsreel.recommender.metarecommender.requesthandler.IdomaarRequestHandler;
import de.dailab.newsreel.recommender.metarecommender.requesthandler.PlistaRequestHandler;
import org.apache.log4j.Logger;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

/**
 * Created by domann on 17.12.15.
 */
public class RequestDelegateHandler extends AbstractHandler implements Serializable {

    private static final Logger log = Logger.getLogger(RequestDelegateHandler.class);

    /* CURRENTLY NOT NEEDED
    private static final Logger evallog = Logger.getLogger(EvalLevel.class);
    static {evallog.setLevel(EvalLevel.EVAL);} */

    private final RequestHandlerProcessor handlerProcessor;
    private final RequestHandler idomaarRequestHandler;
    private final RequestHandler plistaRequestHandler;

    public RequestDelegateHandler() {
        handlerProcessor = new RequestHandlerProcessor();
        idomaarRequestHandler = new IdomaarRequestHandler();
        plistaRequestHandler = new PlistaRequestHandler();
    }

    class RequestHandlerProcessor implements Serializable {

        private static final String PARAMETER_TYPE = "type";
        private static final String PARAMETER_BODY = "body";
        private static final String PARAMETER_PROPERTIES = "properties";
        private static final String PARAMETER_ENTITIES = "entities";

        private String typeValue;
        private String bodyValue;
        private String propertyValue;
        private String entityValue;

        public String perform(Request baseRequest) throws IllegalArgumentException, UnsupportedEncodingException {

            typeValue = baseRequest.getParameter(PARAMETER_TYPE);
            bodyValue = baseRequest.getParameter(PARAMETER_BODY);
            propertyValue = baseRequest.getParameter(PARAMETER_PROPERTIES);
            entityValue = baseRequest.getParameter(PARAMETER_ENTITIES);

            boolean isIdomaarStructure = (typeValue instanceof String) &&
                    (propertyValue instanceof String) &&
                    (entityValue instanceof String);

            if (isIdomaarStructure) {
                decodeIdomaarParameters(baseRequest);
                return idomaarRequestHandler.handleRequest(typeValue, propertyValue, entityValue);
            } else {
                decodePlistaParameters(baseRequest);
                return plistaRequestHandler.handleRequest(typeValue, bodyValue);
            }
        }

        private void decodeIdomaarParameters(Request baseRequest) throws UnsupportedEncodingException {
            if (baseRequest.getContentType().equals("application/x-www-form-urlencoded; charset=utf-8")) {
                propertyValue = URLDecoder.decode(propertyValue, "utf-8");
                entityValue = URLDecoder.decode(entityValue, "utf-8");
            }
        }

        private void decodePlistaParameters(Request baseRequest) throws UnsupportedEncodingException {
            if (baseRequest.getContentType().equals("application/x-www-form-urlencoded; charset=utf-8")) {
                bodyValue = URLDecoder.decode(bodyValue, "utf-8");
            }
        }

        public String getTypeValue() {
            return typeValue;
        }
    }


    @Override
    public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response)
            throws IOException, ServletException {

        try{
            doHandle(baseRequest, request, response);
        } catch (Throwable th) {
            log.warn(th.getMessage());
            th.printStackTrace();
        }

    }

    public void doHandle(Request baseRequest, HttpServletRequest request, HttpServletResponse response)
            throws Exception {
        long start = System.currentTimeMillis();
        // handler can handle POST messages only.
        if (baseRequest.getMethod().equals("POST")) {
            if (baseRequest.getContentLength() < 0) {
                log.debug("Initial Message with no content received.");
                response(response, baseRequest, null, false);
            } else {
                log.debug("Normal Message with content received.");
                String responseText = handlerProcessor.perform(baseRequest);
                response(response, baseRequest, responseText, true);
            }
        }

        /* CURRENTLY NOT NEEDED
        String type = handlerProcessor.getTypeValue();

        if(type.equals(PlistaItem.TYPE_RECOMMENDATION_REQUEST)) {
            long end = System.currentTimeMillis();
            evallog.log(EvalLevel.EVAL, type + "|" + (end - start));
        }*/
    }

    /**
     * Prepare a response.
     *
     * @param response     A {@link HttpServletResponse} object.
     * @param baseRequest  The initial request.
     * @param reponseText  The response text.
     * @param sendResponse A boolean to set whether the response text should be sent.
     * @throws IOException If something went wrong.
     */
    private void response(HttpServletResponse response, Request baseRequest, String reponseText, boolean sendResponse) throws IOException {

        // configure the response parameters
        response.setContentType("text/html;charset=utf-8");
        response.setStatus(HttpServletResponse.SC_OK);
        baseRequest.setHandled(true);

        if (reponseText != null && sendResponse) {
            response.getWriter().println(reponseText);
            if (reponseText != null && !reponseText.startsWith("handle")) {
                log.debug("send response: " + reponseText);
            }
        }
    }

}
