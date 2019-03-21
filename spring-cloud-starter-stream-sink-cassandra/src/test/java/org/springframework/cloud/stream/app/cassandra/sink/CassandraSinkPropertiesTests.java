/*
 * Copyright 2015-2019 the original author or authors.
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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.cloud.stream.app.cassandra.CassandraClusterProperties;
import org.springframework.cloud.stream.config.SpelExpressionConverterConfiguration;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.integration.cassandra.outbound.CassandraMessageHandler;

/**
 * @author Thomas Risberg
 * @author Artem Bilan
 */
public class CassandraSinkPropertiesTests {

	private AnnotationConfigApplicationContext context;

	@Before
	public void setUp() {
		this.context = new AnnotationConfigApplicationContext();
	}

	@After
	public void tearDown() {
		this.context.close();
	}

	@Test
	public void ttlCanBeCustomized() {
		TestPropertyValues.of("cassandra.ttl:" + 1000).applyTo(this.context);
		this.context.register(Conf.class);
		this.context.refresh();
		CassandraSinkProperties properties = this.context.getBean(CassandraSinkProperties.class);
		assertThat(properties.getTtl(), equalTo(1000));
	}

	@Test
	public void queryTypeCanBeCustomized() {
		TestPropertyValues.of("cassandra.query-type:" + CassandraMessageHandler.Type.UPDATE).applyTo(this.context);
		this.context.register(Conf.class);
		this.context.refresh();
		CassandraSinkProperties properties = this.context.getBean(CassandraSinkProperties.class);
		assertThat(properties.getQueryType(), equalTo(CassandraMessageHandler.Type.UPDATE));
	}

	@Test
	public void ingestQueryCanBeCustomized() {
		String query = "insert into book (isbn, title, author) values (?, ?, ?)";
		TestPropertyValues.of("cassandra.ingest-query:" + query).applyTo(this.context);
		this.context.register(Conf.class);
		this.context.refresh();
		CassandraSinkProperties properties = this.context.getBean(CassandraSinkProperties.class);
		assertThat(properties.getIngestQuery(), equalTo(query));
	}

	@Test
	public void statementExpressionCanBeCustomized() {
		String queryDsl = "Select(FOO.BAR).From(FOO)";
		TestPropertyValues.of("cassandra.statementExpression:" + queryDsl).applyTo(this.context);
		this.context.register(Conf.class);
		this.context.refresh();
		CassandraSinkProperties properties = this.context.getBean(CassandraSinkProperties.class);
		assertThat(properties.getStatementExpression().getExpressionString(), equalTo(queryDsl));
	}

	@Configuration
	@EnableConfigurationProperties({ CassandraClusterProperties.class, CassandraSinkProperties.class })
	@Import(SpelExpressionConverterConfiguration.class)
	static class Conf {

	}

}
