/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.camel.component.restlet;

import java.net.URL;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.JndiRegistry;
import org.apache.camel.support.jsse.KeyManagersParameters;
import org.apache.camel.support.jsse.KeyStoreParameters;
import org.apache.camel.support.jsse.SSLContextParameters;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.junit.Test;

public class RestletHttpsWithSSLContextParametersTest extends RestletTestSupport {
    
    private static final String REQUEST_MESSAGE = 
        "<mail><body>HelloWorld!</body><subject>test</subject><to>x@y.net</to></mail>";
    
    @Override
    protected JndiRegistry createRegistry() throws Exception {
        KeyStoreParameters ksp = new KeyStoreParameters();
        ksp.setResource(this.getClass().getClassLoader().getResource("jsse/localhost.ks").getPath().toString());
        ksp.setPassword("changeit");

        KeyManagersParameters kmp = new KeyManagersParameters();
        kmp.setKeyPassword("changeit");
        kmp.setKeyStore(ksp);

        SSLContextParameters sslContextParameters = new SSLContextParameters();
        sslContextParameters.setKeyManagers(kmp);

        JndiRegistry registry = super.createRegistry();
        registry.bind("mySSLContextParameters", sslContextParameters);

        return registry;
    }

    

    @Override
    protected RouteBuilder createRouteBuilder() {
        return new RouteBuilder() {
            @Override
            public void configure() throws Exception {
                // enable POST support
                from("restlet:https://localhost:" + portNum + "/users/?restletMethods=post&sslContextParameters=#mySSLContextParameters")
                    .process(new Processor() {
                        public void process(Exchange exchange) throws Exception {
                            String body = exchange.getIn().getBody(String.class);
                            assertNotNull(body);
                            assertTrue("Get a wrong request message", body.indexOf(REQUEST_MESSAGE) >= 0);
                            exchange.getOut().setBody("<status>OK</status>");
                            exchange.getOut().setHeader(Exchange.CONTENT_TYPE, "application/xml");
                        }
                    });
            }
        };
    }

    @Test
    public void testPostXml() throws Exception {
        postRequestMessage(REQUEST_MESSAGE);
    }
   
    private void postRequestMessage(String message) throws Exception {
        // ensure jsse clients can validate the self signed dummy localhost cert, 
        // use the server keystore as the trust store for these tests
        URL trustStoreUrl = this.getClass().getClassLoader().getResource("jsse/localhost.ks");
        System.setProperty("javax.net.ssl.trustStore", trustStoreUrl.toURI().getPath());
        
        HttpPost post = new HttpPost("https://localhost:" + portNum + "/users/");
        post.addHeader(Exchange.CONTENT_TYPE, "application/xml");
        post.setEntity(new StringEntity(message));

        HttpResponse response = doExecute(post);
        assertHttpResponse(response, 200, "application/xml");
        String s = context.getTypeConverter().convertTo(String.class, response.getEntity().getContent());
        assertEquals("<status>OK</status>", s);
    }

}
