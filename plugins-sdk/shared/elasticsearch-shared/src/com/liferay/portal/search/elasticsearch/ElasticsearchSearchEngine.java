/**
 * Copyright (c) 2000-present Liferay, Inc. All rights reserved.
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 */

package com.liferay.portal.search.elasticsearch;

import com.liferay.portal.kernel.log.Log;
import com.liferay.portal.kernel.log.LogFactoryUtil;
import com.liferay.portal.kernel.search.BaseSearchEngine;
import com.liferay.portal.kernel.search.BooleanQueryFactory;
import com.liferay.portal.kernel.search.TermQueryFactory;
import com.liferay.portal.search.elasticsearch.connection.ElasticsearchConnectionManager;
import com.liferay.portal.search.elasticsearch.index.IndexFactory;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;

/**
 * @author Michael C. Han
 * @author Allen Ziegenfus
 */
public class ElasticsearchSearchEngine extends BaseSearchEngine {

	@Override
	public BooleanQueryFactory getBooleanQueryFactory() {
		if (_booleanQueryFactory == null) {
			_booleanQueryFactory = new BooleanQueryFactoryImpl();
		}

		return _booleanQueryFactory;
	}

	@Override
	public TermQueryFactory getTermQueryFactory() {
		if (_termQueryFactory == null) {
			_termQueryFactory = new TermQueryFactoryImpl();
		}

		return _termQueryFactory;
	}

	public void initialize(long companyId) {
		try {
			_indexFactory.createIndices(
				_elasticsearchConnectionManager.getAdminClient(), companyId);
		}
		catch (Exception e) {
			throw new IllegalStateException(e);
		}

		ClusterHealthResponse clusterHealthResponse =
			_elasticsearchConnectionManager.getClusterHealthResponse();

		if (clusterHealthResponse.getStatus() == ClusterHealthStatus.RED) {
			throw new IllegalStateException(
				"Unable to initialize Elasticsearch cluster: " +
					clusterHealthResponse);
		}
	}

	public void removeCompany(long companyId) {
		try {
			_indexFactory.deleteIndices(
				_elasticsearchConnectionManager.getAdminClient(), companyId);
		}
		catch (Exception e) {
			if (_log.isWarnEnabled()) {
				_log.warn("Unable to delete index for " + companyId, e);
			}
		}
	}

	public void setElasticsearchConnectionManager(
		ElasticsearchConnectionManager elasticsearchConnectionManager) {

		_elasticsearchConnectionManager = elasticsearchConnectionManager;
	}

	public void setIndexFactory(IndexFactory indexFactory) {
		_indexFactory = indexFactory;
	}

	private static Log _log = LogFactoryUtil.getLog(
		ElasticsearchSearchEngine.class);

	private BooleanQueryFactory _booleanQueryFactory;
	private ElasticsearchConnectionManager _elasticsearchConnectionManager;
	private IndexFactory _indexFactory;
	private TermQueryFactory _termQueryFactory;

}