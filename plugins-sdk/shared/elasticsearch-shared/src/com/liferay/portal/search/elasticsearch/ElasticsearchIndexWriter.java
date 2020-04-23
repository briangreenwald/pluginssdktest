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
import com.liferay.portal.kernel.search.BaseIndexWriter;
import com.liferay.portal.kernel.search.Document;
import com.liferay.portal.kernel.search.Field;
import com.liferay.portal.kernel.search.SearchContext;
import com.liferay.portal.kernel.search.SearchException;
import com.liferay.portal.kernel.util.PortalRunMode;
import com.liferay.portal.search.elasticsearch.connection.ElasticsearchConnectionManager;
import com.liferay.portal.search.elasticsearch.util.DocumentTypes;
import com.liferay.portal.search.elasticsearch.util.ElasticsearchSettings;
import com.liferay.portal.search.elasticsearch.util.LogUtil;
import com.liferay.portal.search.elasticsearch.util.SearchContextUtil;

import java.util.Collection;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequestBuilder;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

/**
 * @author Michael C. Han
 * @author Milen Dyankov
 */
public class ElasticsearchIndexWriter extends BaseIndexWriter {

	@Override
	public void addDocument(SearchContext searchContext, Document document)
		throws SearchException {

		_elasticsearchUpdateDocumentCommand.updateDocument(
			DocumentTypes.LIFERAY, searchContext, document, false);
	}

	@Override
	public void addDocuments(
			SearchContext searchContext, Collection<Document> documents)
		throws SearchException {

		_elasticsearchUpdateDocumentCommand.updateDocuments(
			DocumentTypes.LIFERAY, searchContext, documents, false);
	}

	@Override
	public void deleteDocument(SearchContext searchContext, String uid)
		throws SearchException {

		try {
			Client client = _elasticsearchConnectionManager.getClient();

			DeleteRequestBuilder deleteRequestBuilder = client.prepareDelete(
				ElasticsearchSettings.getIndexPrefix() +
					searchContext.getCompanyId(),
				DocumentTypes.LIFERAY, uid);

			if (PortalRunMode.isTestMode() ||
				SearchContextUtil.isCommitImmediately(searchContext)) {

				deleteRequestBuilder.setRefresh(true);
			}

			DeleteResponse deleteResponse = deleteRequestBuilder.get();

			LogUtil.logActionResponse(_log, deleteResponse);
		}
		catch (Exception e) {
			throw new SearchException("Unable to delete document " + uid, e);
		}
	}

	@Override
	public void deleteDocuments(
			SearchContext searchContext, Collection<String> uids)
		throws SearchException {

		try {
			Client client = _elasticsearchConnectionManager.getClient();

			BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();

			for (String uid : uids) {
				DeleteRequestBuilder deleteRequestBuilder =
					client.prepareDelete(
						ElasticsearchSettings.getIndexPrefix() +
							searchContext.getCompanyId(),
						DocumentTypes.LIFERAY, uid);

				bulkRequestBuilder.add(deleteRequestBuilder);
			}

			if (PortalRunMode.isTestMode() ||
				SearchContextUtil.isCommitImmediately(searchContext)) {

				bulkRequestBuilder.setRefresh(true);
			}

			BulkResponse bulkResponse = bulkRequestBuilder.get();

			LogUtil.logActionResponse(_log, bulkResponse);
		}
		catch (Exception e) {
			throw new SearchException("Unable to delete documents " + uids, e);
		}
	}

	@Override
	public void deletePortletDocuments(
			SearchContext searchContext, String portletId)
		throws SearchException {

		try {
			Client client = _elasticsearchConnectionManager.getClient();

			BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();

			SearchRequestBuilder searchRequestBuilder = client.prepareSearch(
				ElasticsearchSettings.getIndexPrefix() +
					searchContext.getCompanyId());

			searchRequestBuilder.setScroll(
				ElasticsearchSettings.SEARCH_SCROLL_KEEP_ALIVE_TIME_VALUE);
			searchRequestBuilder.setSearchType(SearchType.SCAN);
			searchRequestBuilder.setSize(
				ElasticsearchSettings.SEARCH_SCROLL_SIZE);

			BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

			boolQueryBuilder.must(
				QueryBuilders.termQuery(Field.PORTLET_ID, portletId));

			searchRequestBuilder.setQuery(boolQueryBuilder);

			SearchResponse searchResponse = searchRequestBuilder.get();

			SearchScrollRequestBuilder searchScrollRequestBuilder =
				client.prepareSearchScroll(searchResponse.getScrollId());

			searchScrollRequestBuilder.setScroll(
				ElasticsearchSettings.SEARCH_SCROLL_KEEP_ALIVE_TIME_VALUE);

			while (true) {
				searchResponse = searchScrollRequestBuilder.get();

				SearchHits searchHits = searchResponse.getHits();

				SearchHit[] searchHitArray = searchHits.getHits();

				if (searchHitArray.length == 0) {
					break;
				}

				for (SearchHit searchHit : searchHitArray) {
					DeleteRequestBuilder deleteRequestBuilder =
						client.prepareDelete();

					deleteRequestBuilder.setIndex(searchHit.getIndex());
					deleteRequestBuilder.setId(searchHit.getId());
					deleteRequestBuilder.setType(searchHit.getType());

					bulkRequestBuilder.add(deleteRequestBuilder);
				}
			}

			if (bulkRequestBuilder.numberOfActions() == 0) {
				return;
			}

			BulkResponse bulkResponse =
				bulkRequestBuilder.execute().actionGet();

			LogUtil.logActionResponse(_log, bulkResponse);
		}
		catch (Exception e) {
			throw new SearchException(
				"Unable to delete data for portlet " + portletId, e);
		}
	}

	public void setElasticsearchConnectionManager(
		ElasticsearchConnectionManager elasticsearchConnectionManager) {

		_elasticsearchConnectionManager = elasticsearchConnectionManager;
	}

	public void setElasticsearchUpdateDocumentCommand(
		ElasticsearchUpdateDocumentCommand elasticsearchUpdateDocumentCommand) {

		_elasticsearchUpdateDocumentCommand =
			elasticsearchUpdateDocumentCommand;
	}

	@Override
	public void updateDocument(SearchContext searchContext, Document document)
		throws SearchException {

		_elasticsearchUpdateDocumentCommand.updateDocument(
			DocumentTypes.LIFERAY, searchContext, document, true);
	}

	@Override
	public void updateDocuments(
			SearchContext searchContext, Collection<Document> documents)
		throws SearchException {

		_elasticsearchUpdateDocumentCommand.updateDocuments(
			DocumentTypes.LIFERAY, searchContext, documents, true);
	}

	private static Log _log = LogFactoryUtil.getLog(
		ElasticsearchIndexWriter.class);

	private ElasticsearchConnectionManager _elasticsearchConnectionManager;
	private ElasticsearchUpdateDocumentCommand
		_elasticsearchUpdateDocumentCommand;

}