/*
 * Copyright 2017 the original author or authors.
 *
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

package org.springframework.cloud.stream.app.cassandra.util;

import org.springframework.cassandra.core.QueryOptions;
import org.springframework.cassandra.core.WriteOptions;

import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;
import com.datastax.driver.core.policies.FallthroughRetryPolicy;
import com.datastax.driver.core.policies.LoggingRetryPolicy;
import com.datastax.driver.core.policies.RetryPolicy;

/**
 * The copy of deprecated {@code org.springframework.cassandra.core.RetryPolicy}.
 * <p>
 * Retry Policies associated with Cassandra. This enum type is a shortcut to the predefined
 * {@link RetryPolicy policies} that come with the driver.
 *
 * @author Artem Bilan
 *
 */
public enum CassandraRetryPolicy {

	DEFAULT(DefaultRetryPolicy.INSTANCE),

	DOWNGRADING_CONSISTENCY(DowngradingConsistencyRetryPolicy.INSTANCE),

	FALLTHROUGH(FallthroughRetryPolicy.INSTANCE),

	/**
	 * The {@link com.datastax.driver.core.policies.LoggingRetryPolicy}
	 * is just a decorator for other policies so it can't
	 * be applied to {@link QueryOptions}/{@link WriteOptions}
	 */
	LOGGING(new LoggingRetryPolicy(DefaultRetryPolicy.INSTANCE));

	private final RetryPolicy retryPolicy;

	CassandraRetryPolicy(RetryPolicy retryPolicy) {
		this.retryPolicy = retryPolicy;
	}

	public RetryPolicy toRetryPolicy() {
		return this.retryPolicy;
	}

}
