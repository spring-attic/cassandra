/*
 * Copyright 2017 the original author or authors.
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

package org.springframework.cloud.stream.app.cassandra.util;

import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;


/**
 * Helper to provide an SSL Context that does not validate
 * certificates presented in the SSL handshake.
 *
 * The usual caveats apply.
 *
 * @author Rob Hardt
 * @author Artem Bilan
 */
public class TrustAllSSLContextFactory {

	public static SSLContext getSslContext() throws NoSuchAlgorithmException, KeyManagementException {

		TrustManager[] trustAllCerts = new TrustManager[] {
				new X509TrustManager() {

					@Override
					public X509Certificate[] getAcceptedIssuers() {
						return new X509Certificate[0];
					}

					@Override
					public void checkClientTrusted(java.security.cert.X509Certificate[] certs, String authType) {
					}

					@Override
					public void checkServerTrusted(java.security.cert.X509Certificate[] certs, String authType) {
					}

				}
		};

		SSLContext sc = SSLContext.getInstance("SSL");
		sc.init(null, trustAllCerts, new SecureRandom());
		return sc;
	}

}
