/*
 * Copyright 2015-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.app.cassandra.sink;

import static org.junit.Assert.assertThat;
import static org.springframework.integration.test.matcher.EqualsResultMatcher.equalsResult;
import static org.springframework.integration.test.matcher.EventuallyMatcher.eventually;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.cassandraunit.spring.CassandraUnitDependencyInjectionIntegrationTestExecutionListener;
import org.cassandraunit.spring.EmbeddedCassandra;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.app.cassandra.domain.Book;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.integration.support.json.Jackson2JsonObjectMapper;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.StringUtils;

import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.utils.UUIDs;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

/**
 * @author Artem Bilan
 * @author Thomas Risberg
 * @author Ashu Gairola
 * @author Akos Ratku
 */
@RunWith(SpringRunner.class)
@TestExecutionListeners(mergeMode = TestExecutionListeners.MergeMode.MERGE_WITH_DEFAULTS,
		listeners = CassandraUnitDependencyInjectionIntegrationTestExecutionListener.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE,
		properties = {
				"spring.data.cassandra.keyspaceName=" + CassandraSinkIntegrationTests.CASSANDRA_KEYSPACE,
				"cassandra.cluster.createKeyspace=true"})
@EmbeddedCassandra(configuration = EmbeddedCassandraServerHelper.CASSANDRA_RNDPORT_YML_FILE, timeout = 120000)
@DirtiesContext
public abstract class CassandraSinkIntegrationTests {

	public static final String CASSANDRA_KEYSPACE = "test";

	@Autowired
	protected Sink sink;

	@Autowired
	protected CassandraOperations cassandraTemplate;

	@BeforeClass
	public static void setUp() {
		EmbeddedCassandraServerHelper.getSession();
		System.setProperty("spring.data.cassandra.port", "" + EmbeddedCassandraServerHelper.getNativeTransportPort());
	}

	@AfterClass
	public static void cleanup() {
		System.clearProperty("spring.data.cassandra.port");
	}

	@TestPropertySource(properties = {
			"spring.data.cassandra.schema-action=RECREATE",
			"cassandra.cluster.entity-base-packages=org.springframework.cloud.stream.app.cassandra.domain" })
	//	@Ignore("Looks like Embedded Cassandra is still unstable. Consider to use external for testing")
	public static class CassandraEntityInsertTests extends CassandraSinkIntegrationTests {

		@Test
		public void testInsert() {
			Book book = new Book();
			book.setIsbn(UUIDs.timeBased());
			book.setTitle("Spring Integration Cassandra");
			book.setAuthor("Cassandra Guru");
			book.setPages(521);
			book.setSaleDate(new Date());
			book.setInStock(true);

			this.sink.input().send(new GenericMessage<>(book));

			final Select select = QueryBuilder.select().all().from("book");

			assertThat(1, eventually(equalsResult(() -> cassandraTemplate.select(select, Book.class).size())));

			this.cassandraTemplate.delete(book);
		}

	}

	@TestPropertySource(properties = {
			"cassandra.cluster.init-script=init-db.cql",
			"cassandra.ingest-query=" +
					"insert into book (isbn, title, author, pages, saleDate, inStock) values (?, ?, ?, ?, ?, ?)" })
	//	@Ignore("Looks like Embedded Cassandra is still unstable. Consider to use external for testing")
	public static class CassandraSinkIngestInsertTests extends CassandraSinkIntegrationTests {

		@Test
		public void testIngestQuery() throws Exception {
			List<Book> books = getBookList(5);

			ObjectMapper objectMapper = new ObjectMapper();
			objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
			Jackson2JsonObjectMapper mapper = new Jackson2JsonObjectMapper(objectMapper);

			this.sink.input().send(new GenericMessage<>(mapper.toJson(books)));

			final Select select = QueryBuilder.select().all().from("book");

			assertThat(5, eventually(equalsResult(() -> cassandraTemplate.select(select, Book.class).size())));

			this.cassandraTemplate.truncate(Book.class);
		}

	}

	@TestPropertySource(properties = {
			"cassandra.cluster.init-script=init-db.cql",
			"cassandra.ingest-query=" +
					"update book set inStock = :inStock, author = :author, pages = :pages, " +
					"saleDate = :saleDate, title = :title where isbn = :isbn",
			"cassandra.queryType=UPDATE" })
	public static class CassandraSinkIngestUpdateTests extends CassandraSinkIntegrationTests {

		@Test
		public void testIngestQuery() throws Exception {
			List<Book> books = getBookList(5);

			ObjectMapper objectMapper = new ObjectMapper();
			objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
			Jackson2JsonObjectMapper mapper = new Jackson2JsonObjectMapper(objectMapper);

			this.sink.input().send(new GenericMessage<>(mapper.toJson(books)));

			final Select select = QueryBuilder.select().all().from("book");

			assertThat(5, eventually(equalsResult(() -> cassandraTemplate.select(select, Book.class).size())));

			this.cassandraTemplate.truncate(Book.class);
		}

	}

	@TestPropertySource(properties = {
			"cassandra.cluster.init-script=init-db.cql",
			"cassandra.ingest-query=" +
					"insert into book (isbn, title, author, pages, saleDate, inStock) " +
					"values (:myIsbn, :myTitle, :myAuthor, ?, ?, ?)" })
	//	@Ignore("Looks like Embedded Cassandra is still unstable. Consider to use external for testing")
	public static class CassandraSinkIngestNamedParamsTests extends CassandraSinkIntegrationTests {

		@Test
		public void testIngestQuery() throws Exception {
			List<Book> books = getBookList(5);

			ObjectMapper objectMapper = new ObjectMapper();
			objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
			Jackson2JsonObjectMapper mapper = new Jackson2JsonObjectMapper(objectMapper);

			String booksJsonWithNamedParams = mapper.toJson(books);
			booksJsonWithNamedParams = StringUtils.replace(booksJsonWithNamedParams, "isbn", "myIsbn");
			booksJsonWithNamedParams = StringUtils.replace(booksJsonWithNamedParams, "title", "myTitle");
			booksJsonWithNamedParams = StringUtils.replace(booksJsonWithNamedParams, "author", "myAuthor");
			this.sink.input().send(new GenericMessage<>(booksJsonWithNamedParams));

			final Select select = QueryBuilder.select().all().from("book");

			assertThat(5, eventually(equalsResult(() -> cassandraTemplate.select(select, Book.class).size())));

			this.cassandraTemplate.truncate(Book.class);
		}

	}

	private static List<Book> getBookList(int numBooks) {

		List<Book> books = new ArrayList<>();

		Book b;
		for (int i = 0; i < numBooks; i++) {
			b = new Book();
			b.setIsbn(UUID.randomUUID());
			b.setTitle("Spring Cloud Data Flow Guide");
			b.setAuthor("SCDF Guru");
			b.setPages(i * 10 + 5);
			b.setInStock(true);
			b.setSaleDate(new Date());
			books.add(b);
		}

		return books;
	}

	@SpringBootApplication
	static class CassandraSinkApplication {

		public static void main(String[] args) {
			SpringApplication.run(CassandraSinkApplication.class, args);
		}

	}

}
