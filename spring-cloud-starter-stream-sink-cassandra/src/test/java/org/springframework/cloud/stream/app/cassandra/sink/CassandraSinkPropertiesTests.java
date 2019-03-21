/*
 * Copyright 2015-2017 the original author or authors.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.app.cassandra.sink;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.util.EnvironmentTestUtils;
import org.springframework.cloud.stream.app.cassandra.CassandraProperties;
import org.springframework.cloud.stream.app.cassandra.util.CassandraRetryPolicy;
import org.springframework.cloud.stream.config.SpelExpressionConverterConfiguration;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.integration.cassandra.outbound.CassandraMessageHandler;

import com.datastax.driver.core.ConsistencyLevel;

/**
 * @author Thomas Risberg
 * @author Artem Bilan
 */
public class CassandraSinkPropertiesTests {

	@Test
	public void consistencyLevelCanBeCustomized() {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		EnvironmentTestUtils.addEnvironment(context, "CASSANDRA_CONSISTENCY_LEVEL:" + ConsistencyLevel.LOCAL_QUORUM);
		context.register(Conf.class);
		context.refresh();
		CassandraSinkProperties properties = context.getBean(CassandraSinkProperties.class);
		assertThat(properties.getConsistencyLevel(), equalTo(ConsistencyLevel.LOCAL_QUORUM));
		context.close();
	}

	@Test
	public void retryPolicyCanBeCustomized() {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		EnvironmentTestUtils.addEnvironment(context, "cassandra.retry-policy:" + CassandraRetryPolicy.DOWNGRADING_CONSISTENCY);
		context.register(Conf.class);
		context.refresh();
		CassandraSinkProperties properties = context.getBean(CassandraSinkProperties.class);
		assertThat(properties.getRetryPolicy(), equalTo(CassandraRetryPolicy.DOWNGRADING_CONSISTENCY));
		context.close();
	}

	@Test
	public void ttlCanBeCustomized() {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		EnvironmentTestUtils.addEnvironment(context, "cassandra.ttl:" + 1000);
		context.register(Conf.class);
		context.refresh();
		CassandraSinkProperties properties = context.getBean(CassandraSinkProperties.class);
		assertThat(properties.getTtl(), equalTo(1000));
		context.close();
	}

	@Test
	public void queryTypeCanBeCustomized() {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		EnvironmentTestUtils.addEnvironment(context, "cassandra.query-type:" + CassandraMessageHandler.Type.UPDATE);
		context.register(Conf.class);
		context.refresh();
		CassandraSinkProperties properties = context.getBean(CassandraSinkProperties.class);
		assertThat(properties.getQueryType(), equalTo(CassandraMessageHandler.Type.UPDATE));
		context.close();
	}

	@Test
	public void ingestQueryCanBeCustomized() {
		String query = "insert into book (isbn, title, author) values (?, ?, ?)";
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		EnvironmentTestUtils.addEnvironment(context, "cassandra.ingest-query:" + query);
		context.register(Conf.class);
		context.refresh();
		CassandraSinkProperties properties = context.getBean(CassandraSinkProperties.class);
		assertThat(properties.getIngestQuery(), equalTo(query));
		context.close();
	}

	@Test
	public void statementExpressionCanBeCustomized() {
		String queryDsl = "Select(FOO.BAR).From(FOO)";
		Expression expression = new SpelExpressionParser().parseExpression(queryDsl);
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		EnvironmentTestUtils.addEnvironment(context, "cassandra.statementExpression:" + queryDsl);
		context.register(Conf.class);
		context.refresh();
		CassandraSinkProperties properties = context.getBean(CassandraSinkProperties.class);
		assertThat(properties.getStatementExpression().getExpressionString(), equalTo(expression.getExpressionString()));
		context.close();
	}

	@Test
	public void sslCanBeCustomized() {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		EnvironmentTestUtils.addEnvironment(context, "cassandra.cluster.useSsl:" + true);
		EnvironmentTestUtils.addEnvironment(context, "cassandra.cluster.skipSslValidation:" + true);
		context.register(Conf.class);
		context.refresh();
		CassandraProperties properties = context.getBean(CassandraProperties.class);
		assertTrue(properties.isUseSsl());
		assertTrue(properties.isSkipSslValidation());
		context.close();
	}

	@Configuration
	@EnableConfigurationProperties({ CassandraProperties.class, CassandraSinkProperties.class })
	@Import(SpelExpressionConverterConfiguration.class)
	static class Conf {

	}

}
