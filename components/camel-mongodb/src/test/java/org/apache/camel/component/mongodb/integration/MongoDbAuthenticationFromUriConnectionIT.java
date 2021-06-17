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

package org.apache.camel.component.mongodb.integration;

import java.util.Collections;

import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MongoDbAuthenticationFromUriConnectionIT extends AbstractMongoDbITSupport {

    public static final String MY_OTHER_DB = "myOtherDb";
    public static final String MY_OTHER_COLL = "myOtherColl";
    public static final String MY_OTHER_USER = "myUser";
    public static final String MY_OTHER_PASSWD = "myPasswd";
    private static MongoDatabase myOtherDb;
    private static MongoCollection<Document> otherColl;

    @Override
    public void doPreSetup() throws Exception {
        // create user in db
        super.doPreSetup();
        createOtherUser();
    }

    @Override
    protected CamelContext createCamelContext() throws Exception {
        //This is necessary to avoid creating connection bean for the mongodb component and test credentials instead
        @SuppressWarnings("deprecation")
        CamelContext ctx = new DefaultCamelContext();
        ctx.getPropertiesComponent().setLocation("classpath:mongodb.test.properties");
        return ctx;
    }

    // create a single user on another db to test authentication using hosts parameter
    protected void createOtherUser() {
        myOtherDb = mongo.getDatabase(MY_OTHER_DB);
        MongoCollection<Document> usersCollection = myOtherDb.getCollection("system.users");
        if (usersCollection.countDocuments() == 0) {

            BasicDBObject createUserCommand = new BasicDBObject("createUser", MY_OTHER_USER)
                    .append("pwd", MY_OTHER_PASSWD)
                    .append("roles",
                            Collections.singletonList(new BasicDBObject("role", "readWrite").append("db", MY_OTHER_DB)));
            myOtherDb.runCommand(createUserCommand);
        }
        String doc = "{\"_id\":\"1\", \"scientist\":\"Einstein\"}";
        otherColl = myOtherDb.getCollection(MY_OTHER_COLL, Document.class);
        otherColl.insertOne(Document.parse(doc));
    }

    @Test
    public void checkAuthenticationFromQuery() {
        Object result = template.requestBody("direct:authenticate", "irrelevantBody");
        assertTrue(result instanceof Long, "Result is not of type Long");
        assertEquals(1L, result, MY_OTHER_COLL + " collection should contain 1 record");
        otherColl.drop();
    }

    @Override
    protected RouteBuilder createRouteBuilder() throws Exception {
        return new RouteBuilder() {
            public void configure() {
                String params = "database=" + MY_OTHER_DB
                                + "&collection=" + MY_OTHER_COLL
                                + "&operation=count"
                                + "&hosts=" + service.getConnectionAddress()
                                + "&username=" + MY_OTHER_USER
                                + "&password=" + MY_OTHER_PASSWD;
                from("direct:authenticate")
                        .to("mongodb:?" + params);

            }
        };
    }
}
