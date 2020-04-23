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
import com.liferay.portal.kernel.search.Document;
import com.liferay.portal.kernel.search.DocumentImpl;
import com.liferay.portal.kernel.search.Field;
import com.liferay.portal.kernel.search.SearchContext;
import com.liferay.portal.kernel.search.SearchException;
import com.liferay.portal.search.elasticsearch.connection.ElasticsearchConnectionManager;
import com.liferay.portal.search.elasticsearch.util.DocumentTypes;
import com.liferay.portal.search.elasticsearch.util.ElasticsearchSettings;
import com.liferay.portal.search.elasticsearch.util.LogUtil;
import com.liferay.portal.util.PortletKeys;

import java.util.Collection;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequestBuilder;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

/**
 * @author Michael C. Han
 */
public class ElasticsearchSpellCheckIndexWriter
	extends BaseGenericSpellCheckIndexWriter {

	@Override
	public void clearQuerySuggestionDictionaryIndexes(
			SearchContext searchContext)
		throws SearchException {

		try {
			deleteIndices(searchContext, DocumentTypes.KEYWORD_QUERY);
		}
		catch (Exception e) {
			throw new SearchException("Unable to clear query suggestions", e);
		}
	}

	@Override
	public void clearSpellCheckerDictionaryIndexes(SearchContext searchContext)
		throws SearchException {

		try {
			deleteIndices(searchContext, DocumentTypes.SPELL_CHECK);
		}
		catch (Exception e) {
			throw new SearchException("Unable to to clear spell checks", e);
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

	protected void addDocument(
			String documentType, SearchContext searchContext, Document document)
		throws SearchException {

		_elasticsearchUpdateDocumentCommand.updateDocument(
			documentType, searchContext, document, false);
	}

	protected void addDocuments(
			String documentType, SearchContext searchContext,
			Collection<Document> documents)
		throws SearchException {

		_elasticsearchUpdateDocumentCommand.updateDocuments(
			documentType, searchContext, documents, false);
	}

	@Override
	protected Document createDocument(
		long companyId, long groupId, String languageId, String keywords,
		float weight, String keywordFieldName, String typeFieldValue,
		int maxNGramLength) {

		Document document = new DocumentImpl();

		document.addKeyword(Field.COMPANY_ID, companyId);
		document.addKeyword(Field.GROUP_ID, groupId);

		String localizedName = DocumentImpl.getLocalizedName(
			languageId, keywordFieldName);

		document.addKeyword(localizedName, keywords);

		document.addKeyword(Field.PORTLET_ID, PortletKeys.SEARCH);
		document.addKeyword(Field.PRIORITY, String.valueOf(weight));
		document.addKeyword(Field.UID, getUID(companyId, languageId, keywords));

		return document;
	}

	protected void deleteIndices(SearchContext searchContext, String indexType)
		throws Exception {

		Client client = _elasticsearchConnectionManager.getClient();

		BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();

		SearchRequestBuilder searchRequestBuilder = client.prepareSearch(
			ElasticsearchSettings.getIndexPrefix() +
				searchContext.getCompanyId());

		searchRequestBuilder.setQuery(QueryBuilders.matchAllQuery());
		searchRequestBuilder.setScroll(
			ElasticsearchSettings.SEARCH_SCROLL_KEEP_ALIVE_TIME_VALUE);
		searchRequestBuilder.setSearchType(SearchType.SCAN);
		searchRequestBuilder.setSize(ElasticsearchSettings.SEARCH_SCROLL_SIZE);
		searchRequestBuilder.setTypes(indexType);

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

		BulkResponse bulkResponse = bulkRequestBuilder.execute().actionGet();

		LogUtil.logActionResponse(_log, bulkResponse);
	}

	private static Log _log = LogFactoryUtil.getLog(
		ElasticsearchSpellCheckIndexWriter.class);

	private ElasticsearchConnectionManager _elasticsearchConnectionManager;
	private ElasticsearchUpdateDocumentCommand
		_elasticsearchUpdateDocumentCommand;

}