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

import com.liferay.portal.kernel.dao.orm.QueryUtil;
import com.liferay.portal.kernel.dao.search.SearchPaginationUtil;
import com.liferay.portal.kernel.log.Log;
import com.liferay.portal.kernel.log.LogFactoryUtil;
import com.liferay.portal.kernel.search.BaseIndexSearcher;
import com.liferay.portal.kernel.search.Document;
import com.liferay.portal.kernel.search.DocumentImpl;
import com.liferay.portal.kernel.search.Field;
import com.liferay.portal.kernel.search.Hits;
import com.liferay.portal.kernel.search.HitsImpl;
import com.liferay.portal.kernel.search.Query;
import com.liferay.portal.kernel.search.QueryConfig;
import com.liferay.portal.kernel.search.SearchContext;
import com.liferay.portal.kernel.search.SearchException;
import com.liferay.portal.kernel.search.Sort;
import com.liferay.portal.kernel.search.facet.Facet;
import com.liferay.portal.kernel.search.facet.collector.FacetCollector;
import com.liferay.portal.kernel.util.ArrayUtil;
import com.liferay.portal.kernel.util.StringBundler;
import com.liferay.portal.kernel.util.StringPool;
import com.liferay.portal.kernel.util.StringUtil;
import com.liferay.portal.kernel.util.Validator;
import com.liferay.portal.search.elasticsearch.connection.ElasticsearchConnectionManager;
import com.liferay.portal.search.elasticsearch.facet.ElasticsearchFacetFieldCollector;
import com.liferay.portal.search.elasticsearch.facet.FacetProcessor;
import com.liferay.portal.search.elasticsearch.util.DocumentTypes;
import com.liferay.portal.search.elasticsearch.util.ElasticsearchSettings;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.time.StopWatch;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.lucene.search.function.FieldValueFactorFunction;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.DecayFunctionBuilder;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.index.query.functionscore.fieldvaluefactor.FieldValueFactorFunctionBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.highlight.HighlightField;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortOrder;

/**
 * @author Michael C. Han
 * @author Milen Dyankov
 */
public class ElasticsearchIndexSearcher extends BaseIndexSearcher {

	@Override
	public Hits search(SearchContext searchContext, Query query)
		throws SearchException {

		StopWatch stopWatch = new StopWatch();

		stopWatch.start();

		try {
			int total = (int)searchCount(searchContext, query);

			int start = searchContext.getStart();
			int end = searchContext.getEnd();

			if ((end == QueryUtil.ALL_POS) && (start == QueryUtil.ALL_POS)) {
				start = 0;
				end = total;
			}

			int[] startAndEnd = SearchPaginationUtil.calculateStartAndEnd(
				start, end, total);

			start = startAndEnd[0];
			end = startAndEnd[1];

			SearchResponse searchResponse = doSearch(
				searchContext, query, start, end);

			Hits hits = processSearchResponse(
				searchResponse, searchContext, query);

			hits.setStart(stopWatch.getStartTime());

			return hits;
		}
		catch (Exception e) {
			if (_log.isWarnEnabled()) {
				_log.warn(e, e);
			}

			if (!_swallowException) {
				throw new SearchException(e.getMessage(), e);
			}

			return new HitsImpl();
		}
		finally {
			if (_log.isInfoEnabled()) {
				stopWatch.stop();

				_log.info(
					"Searching " + query.toString() + " took " +
						stopWatch.getTime() + " ms");
			}
		}
	}

	@Deprecated
	@Override
	public Hits search(
			String searchEngineId, long companyId, Query query, Sort[] sorts,
			int start, int end)
		throws SearchException {

		SearchContext searchContext = new SearchContext();

		searchContext.setCompanyId(companyId);
		searchContext.setEnd(end);
		searchContext.setSearchEngineId(searchEngineId);
		searchContext.setSorts(sorts);
		searchContext.setStart(start);

		return search(searchContext, query);
	}

	public long searchCount(SearchContext searchContext, Query query)
		throws SearchException {

		StopWatch stopWatch = new StopWatch();

		stopWatch.start();

		try {
			SearchResponse searchResponse = doSearch(
				searchContext, query, searchContext.getStart(),
				searchContext.getEnd(), true);

			SearchHits searchHits = searchResponse.getHits();

			return searchHits.getTotalHits();
		}
		catch (Exception e) {
			if (_log.isWarnEnabled()) {
				_log.warn(e, e);
			}

			if (!_swallowException) {
				throw new SearchException(e.getMessage(), e);
			}

			return 0;
		}
		finally {
			if (_log.isInfoEnabled()) {
				stopWatch.stop();

				_log.info(
					"Searching " + query.toString() + " took " +
						stopWatch.getTime() + " ms");
			}
		}
	}

	public void setElasticsearchConnectionManager(
		ElasticsearchConnectionManager elasticsearchConnectionManager) {

		_elasticsearchConnectionManager = elasticsearchConnectionManager;
	}

	public void setFacetProcessor(
		FacetProcessor<SearchRequestBuilder> facetProcessor) {

		_facetProcessor = facetProcessor;
	}

	public void setSwallowException(boolean swallowException) {
		_swallowException = swallowException;
	}

	protected void addFacets(
		SearchRequestBuilder searchRequestBuilder,
		SearchContext searchContext) {

		Map<String, Facet> facetsMap = searchContext.getFacets();

		for (Facet facet : facetsMap.values()) {
			if (facet.isStatic()) {
				continue;
			}

			_facetProcessor.processFacet(searchRequestBuilder, facet);
		}
	}

	protected void addHighlightedField(
		SearchRequestBuilder searchRequestBuilder, QueryConfig queryConfig,
		String fieldName) {

		searchRequestBuilder.addHighlightedField(
			fieldName, queryConfig.getHighlightFragmentSize(),
			queryConfig.getHighlightSnippetSize());

		String localizedFieldName = DocumentImpl.getLocalizedName(
			queryConfig.getLocale(), fieldName);

		searchRequestBuilder.addHighlightedField(
			localizedFieldName, queryConfig.getHighlightFragmentSize(),
			queryConfig.getHighlightSnippetSize());
	}

	protected void addHighlights(
		SearchRequestBuilder searchRequestBuilder, QueryConfig queryConfig) {

		if (!queryConfig.isHighlightEnabled()) {
			return;
		}

		addHighlightedField(
			searchRequestBuilder, queryConfig, Field.ASSET_CATEGORY_TITLES);
		addHighlightedField(searchRequestBuilder, queryConfig, Field.CONTENT);
		addHighlightedField(
			searchRequestBuilder, queryConfig, Field.DESCRIPTION);
		addHighlightedField(searchRequestBuilder, queryConfig, Field.TITLE);

		searchRequestBuilder.setHighlighterRequireFieldMatch(true);
	}

	protected void addPagination(
		SearchRequestBuilder searchRequestBuilder, int start, int end) {

		searchRequestBuilder.setFrom(start);
		searchRequestBuilder.setSize(end - start);
	}

	protected void addPreference(
		SearchRequestBuilder searchRequestBuilder,
		SearchContext searchContext) {

		String preference = (String)searchContext.getAttribute("preference");

		if (Validator.isNotNull(preference)) {
			searchRequestBuilder.setPreference(preference);
		}
	}

	protected void addSelectedFields(
		SearchRequestBuilder searchRequestBuilder, QueryConfig queryConfig) {

		String[] selectedFieldNames = (String[])queryConfig.getAttribute(
			"selectedFieldNames");

		if (ArrayUtil.isEmpty(selectedFieldNames)) {
			searchRequestBuilder.addField(StringPool.STAR);
		}
		else {
			searchRequestBuilder.addFields(selectedFieldNames);
		}
	}

	protected void addSnippets(
		Document document, Set<String> queryTerms,
		Map<String, HighlightField> highlightFields, String fieldName,
		Locale locale) {

		String snippet = StringPool.BLANK;

		String localizedContentName = DocumentImpl.getLocalizedName(
			locale, fieldName);

		String snippetFieldName = localizedContentName;

		HighlightField highlightField = highlightFields.get(
			localizedContentName);

		if (highlightField == null) {
			highlightField = highlightFields.get(fieldName);

			snippetFieldName = fieldName;
		}

		if (highlightField != null) {
			Text[] texts = highlightField.fragments();

			StringBundler sb = new StringBundler(texts.length * 2);

			for (Text text : texts) {
				sb.append(text);
				sb.append(StringPool.TRIPLE_PERIOD);
			}

			snippet = sb.toString();
		}

		if (!snippet.equals(StringPool.BLANK)) {
			Matcher matcher = _pattern.matcher(snippet);

			while (matcher.find()) {
				queryTerms.add(matcher.group(1));
			}

			snippet = StringUtil.replace(snippet, "<em>", StringPool.BLANK);
			snippet = StringUtil.replace(snippet, "</em>", StringPool.BLANK);
		}

		document.addText(
			Field.SNIPPET.concat(StringPool.UNDERLINE).concat(snippetFieldName),
			snippet);
	}

	protected void addSnippets(
		SearchHit hit, Document document, QueryConfig queryConfig,
		Set<String> queryTerms) {

		if (!queryConfig.isHighlightEnabled()) {
			return;
		}

		Map<String, HighlightField> highlightFields = hit.getHighlightFields();

		if ((highlightFields == null) || highlightFields.isEmpty()) {
			return;
		}

		addSnippets(
			document, queryTerms, highlightFields, Field.ASSET_CATEGORY_TITLES,
			queryConfig.getLocale());
		addSnippets(
			document, queryTerms, highlightFields, Field.CONTENT,
			queryConfig.getLocale());
		addSnippets(
			document, queryTerms, highlightFields, Field.DESCRIPTION,
			queryConfig.getLocale());
		addSnippets(
			document, queryTerms, highlightFields, Field.TITLE,
			queryConfig.getLocale());
	}

	protected void addSort(
		SearchRequestBuilder searchRequestBuilder, Sort[] sorts) {

		if ((sorts == null) || (sorts.length == 0)) {
			return;
		}

		Set<String> sortFieldNames = new HashSet<String>();

		for (Sort sort : sorts) {
			if (sort == null) {
				continue;
			}

			String sortFieldName = getSortName(sort, "_score");

			if (sortFieldNames.contains(sortFieldName)) {
				continue;
			}

			sortFieldNames.add(sortFieldName);

			SortOrder sortOrder = SortOrder.ASC;

			if (sort.isReverse() || sortFieldName.equals("_score")) {
				sortOrder = SortOrder.DESC;
			}

			SortBuilder sortBuilder = null;

			if (sortFieldName.equals("_score")) {
				sortBuilder = new ScoreSortBuilder();
			}
			else {
				FieldSortBuilder fieldSortBuilder = new FieldSortBuilder(
					sortFieldName);

				fieldSortBuilder.ignoreUnmapped(true);

				sortBuilder = fieldSortBuilder;
			}

			sortBuilder.order(sortOrder);

			searchRequestBuilder.addSort(sortBuilder);
		}
	}

	protected SearchResponse doSearch(
			SearchContext searchContext, Query query, int start, int end)
		throws Exception {

		return doSearch(searchContext, query, start, end, false);
	}

	protected SearchResponse doSearch(
		SearchContext searchContext, Query query, int start, int end,
		boolean count) {

		Client client = _elasticsearchConnectionManager.getClient();

		QueryConfig queryConfig = query.getQueryConfig();

		String[] indices = StringUtil.split(
			(String)queryConfig.getAttribute("indices"));

		SearchRequestBuilder searchRequestBuilder = null;

		if (indices.length == 0) {
			searchRequestBuilder = client.prepareSearch(
				ElasticsearchSettings.getIndexPrefix() +
					searchContext.getCompanyId());
		}
		else {
			searchRequestBuilder = client.prepareSearch(indices);
		}

		if (!count) {
			addFacets(searchRequestBuilder, searchContext);
			addHighlights(searchRequestBuilder, queryConfig);
			addPagination(searchRequestBuilder, start, end);
			addPreference(searchRequestBuilder, searchContext);
			addSelectedFields(searchRequestBuilder, queryConfig);
			addSort(searchRequestBuilder, searchContext.getSorts());

			searchRequestBuilder.setTrackScores(queryConfig.isScoreEnabled());
		}
		else {
			searchRequestBuilder.setSize(0);
		}

		String queryString = preprocessQuery(query.toString());

		QueryBuilder queryBuilder = (QueryBuilder)query.getWrappedQuery();

		if ((queryConfig.getAttribute("boostFields") != null) ||
			(queryConfig.getAttribute("boostFunctions") != null)) {

			if (queryConfig.getAttribute("boostFields") != null) {
				LinkedHashMap<String, Float> boostFields =
					(LinkedHashMap<String, Float>)queryConfig.getAttribute(
						"boostFields");

				MultiMatchQueryBuilder multiMatchQueryBuilder =
					QueryBuilders.multiMatchQuery(queryString);

				for (Map.Entry<String, Float> boostField :
						boostFields.entrySet()) {

					multiMatchQueryBuilder.field(
						boostField.getKey(), boostField.getValue());
				}

				queryBuilder = multiMatchQueryBuilder;
			}

			if (queryConfig.getAttribute("boostFunctions") != null) {
				FunctionScoreQueryBuilder functionScoreQuery =
					QueryBuilders.functionScoreQuery(queryBuilder);

				LinkedHashMap<String, LinkedHashMap<String, Object>> boosts =
					(LinkedHashMap<String, LinkedHashMap<String, Object>>)
						queryConfig.getAttribute("boostFunctions");

				for (Map.Entry<String, LinkedHashMap<String, Object>> boost :
						boosts.entrySet()) {

					functionScoreQuery.add(
						getFunctionScoreQuery(
							boost.getKey(), boost.getValue()));
				}

				queryBuilder = functionScoreQuery;
			}
		}

		if (queryConfig.getAttribute("filterQuery") != null) {
			BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

			Query filterQuery = (Query)queryConfig.getAttribute("filterQuery");

			boolQueryBuilder.filter(
				(QueryBuilder)filterQuery.getWrappedQuery());

			boolQueryBuilder.must(queryBuilder);

			queryBuilder = boolQueryBuilder;
		}

		searchRequestBuilder.setQuery(queryBuilder);

		String[] documentTypes = StringUtil.split(
			(String)queryConfig.getAttribute("documentTypes"));

		if (documentTypes.length == 0) {
			searchRequestBuilder.setTypes(DocumentTypes.LIFERAY);
		}
		else {
			searchRequestBuilder.setTypes(documentTypes);
		}

		SearchResponse searchResponse = searchRequestBuilder.get();

		if (_log.isInfoEnabled()) {
			_log.info(
				"The search engine processed " + queryBuilder.toString() +
					"in " + searchResponse.getTook());
		}

		return searchResponse;
	}

	protected ScoreFunctionBuilder getFunctionScoreQuery(
		String functionName, LinkedHashMap<String, Object> values) {

		if (functionName.endsWith("Decay")) {
			DecayFunctionBuilder functionBuilder = null;

			String fieldName = (String)values.get("fieldName");
			Object scale = values.get("scale");

			if (functionName.equals("exponentialDecay")) {
				functionBuilder =
					ScoreFunctionBuilders.exponentialDecayFunction(
						fieldName, scale);
			}
			else if (functionName.equals("gaussDecay")) {
				functionBuilder = ScoreFunctionBuilders.gaussDecayFunction(
					fieldName, scale);
			}
			else if (functionName.equals("linearDecay")) {
				functionBuilder = ScoreFunctionBuilders.linearDecayFunction(
					fieldName, scale);
			}

			Double decay = (Double)values.get("decay");

			if (decay != null) {
				functionBuilder.setDecay(decay);
			}

			String multiValueMode = (String)values.get("multiValueMode");

			if (multiValueMode != null) {
				functionBuilder.setMultiValueMode(multiValueMode);
			}

			Object offset = values.get("offset");

			if (offset != null) {
				functionBuilder.setOffset(offset);
			}

			Float weight = (Float)values.get("weight");

			if (weight != null) {
				functionBuilder.setWeight(weight);
			}

			return functionBuilder;
		}
		else if (functionName.equals("fieldValueFactor")) {
			String fieldName = (String)values.get("fieldName");

			FieldValueFactorFunctionBuilder functionBuilder =
				ScoreFunctionBuilders.fieldValueFactorFunction(fieldName);

			Float boostFactor = (Float)values.get("factor");

			if (boostFactor != null) {
				functionBuilder.factor(boostFactor);
			}

			Double missing = (Double)values.get("missing");

			if (missing != null) {
				functionBuilder.missing(missing);
			}

			String modifier = (String)values.get("modifier");

			if (modifier != null) {
				functionBuilder.modifier(
					FieldValueFactorFunction.Modifier.valueOf(modifier));
			}

			Float weight = (Float)values.get("weight");

			if (weight != null) {
				functionBuilder.setWeight(weight);
			}

			return functionBuilder;
		}

		return null;
	}

	protected String getSortName(Sort sort, String scoreFieldName) {
		if (sort.getType() == Sort.SCORE_TYPE) {
			return scoreFieldName;
		}

		String fieldName = sort.getFieldName();

		if (fieldName.endsWith(_SORTABLE_FIELD_SUFFIX)) {
			return fieldName;
		}

		String sortFieldName = null;

		if (DocumentImpl.isSortableTextField(fieldName) ||
			(sort.getType() != Sort.STRING_TYPE)) {

			sortFieldName = DocumentImpl.getSortableFieldName(fieldName);
		}

		if (Validator.isNull(sortFieldName)) {
			sortFieldName = scoreFieldName;
		}

		return sortFieldName;
	}

	protected String preprocessQuery(String queryString) {
		return StringUtil.replace(
			queryString, StringPool.SLASH, StringPool.DOUBLE_UNDERLINE);
	}

	protected Document processSearchHit(SearchHit hit) {
		Document document = new DocumentImpl();

		Map<String, SearchHitField> searchHitFields = hit.getFields();

		for (Map.Entry<String, SearchHitField> entry :
				searchHitFields.entrySet()) {

			SearchHitField searchHitField = entry.getValue();

			Collection<Object> fieldValues = searchHitField.getValues();

			Field field = new Field(
				entry.getKey(),
				ArrayUtil.toStringArray(
					fieldValues.toArray(new Object[fieldValues.size()])));

			document.add(field);
		}

		return document;
	}

	protected Hits processSearchResponse(
		SearchResponse searchResponse, SearchContext searchContext,
		Query query) {

		SearchHits searchHits = searchResponse.getHits();

		updateFacetCollectors(searchContext, searchResponse);

		Hits hits = new HitsImpl();

		List<Document> documents = new ArrayList<Document>();
		Set<String> queryTerms = new HashSet<String>();
		List<Float> scores = new ArrayList<Float>();

		if (searchHits.totalHits() > 0) {
			SearchHit[] searchHitsArray = searchHits.getHits();

			for (SearchHit searchHit : searchHitsArray) {
				Document document = processSearchHit(searchHit);

				documents.add(document);

				scores.add(searchHit.getScore());

				addSnippets(
					searchHit, document, searchContext.getQueryConfig(),
					queryTerms);
			}
		}

		hits.setDocs(documents.toArray(new Document[documents.size()]));
		hits.setLength((int)searchHits.getTotalHits());
		hits.setQuery(query);
		hits.setQueryTerms(queryTerms.toArray(new String[queryTerms.size()]));
		hits.setScores(ArrayUtil.toFloatArray(scores));

		TimeValue timeValue = searchResponse.getTook();

		hits.setSearchTime((float)timeValue.getSecondsFrac());

		return hits;
	}

	protected void updateFacetCollectors(
		SearchContext searchContext, SearchResponse searchResponse) {

		Aggregations aggregations = searchResponse.getAggregations();

		if (aggregations == null) {
			return;
		}

		Map<String, Aggregation> aggregationsMap = aggregations.getAsMap();

		Map<String, Facet> facetsMap = searchContext.getFacets();

		for (Facet facet : facetsMap.values()) {
			if (facet.isStatic()) {
				continue;
			}

			Aggregation aggregation = aggregationsMap.get(facet.getFieldName());

			FacetCollector facetCollector =
				new ElasticsearchFacetFieldCollector(aggregation);

			facet.setFacetCollector(facetCollector);
		}
	}

	private static final String _SORTABLE_FIELD_SUFFIX = "sortable";

	private static Log _log = LogFactoryUtil.getLog(
		ElasticsearchIndexSearcher.class);

	private ElasticsearchConnectionManager _elasticsearchConnectionManager;
	private FacetProcessor<SearchRequestBuilder> _facetProcessor;
	private Pattern _pattern = Pattern.compile("<em>(.*?)</em>");
	private boolean _swallowException;

}