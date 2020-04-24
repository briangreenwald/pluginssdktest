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

import com.liferay.portal.kernel.search.BaseBooleanQueryImpl;
import com.liferay.portal.kernel.search.BooleanClause;
import com.liferay.portal.kernel.search.BooleanClauseOccur;
import com.liferay.portal.kernel.search.BooleanClauseOccurImpl;
import com.liferay.portal.kernel.search.Query;
import com.liferay.portal.kernel.util.StringUtil;
import com.liferay.portal.kernel.util.Validator;
import com.liferay.portal.search.elasticsearch.analysis.FieldQueryFactoryUtil;
import com.liferay.portal.search.elasticsearch.util.ElasticsearchConstants;

import java.util.Collections;
import java.util.List;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;

/**
 * @author Jenny Chen
 */
public class BooleanQueryImpl extends BaseBooleanQueryImpl {

	public BooleanQueryImpl() {
		_boolQuery = QueryBuilders.boolQuery();
	}

	@Override
	public void add(Query query, BooleanClauseOccur booleanClauseOccur) {
		if (booleanClauseOccur.equals(BooleanClauseOccur.MUST)) {
			_boolQuery.must((QueryBuilder)query.getWrappedQuery());
		}
		else if (booleanClauseOccur.equals(BooleanClauseOccur.MUST_NOT)) {
			_boolQuery.mustNot((QueryBuilder)query.getWrappedQuery());
		}
		else if (booleanClauseOccur.equals(BooleanClauseOccur.SHOULD)) {
			_boolQuery.should((QueryBuilder)query.getWrappedQuery());
		}
	}

	@Override
	public void add(Query query, String occur) {
		BooleanClauseOccur booleanClauseOccur = new BooleanClauseOccurImpl(
			occur);

		add(query, booleanClauseOccur);
	}

	@Override
	public void addExactTerm(String field, boolean value) {
		addExactTerm(field, String.valueOf(value));
	}

	@Override
	public void addExactTerm(String field, Boolean value) {
		addExactTerm(field, String.valueOf(value));
	}

	@Override
	public void addExactTerm(String field, double value) {
		addExactTerm(field, String.valueOf(value));
	}

	@Override
	public void addExactTerm(String field, Double value) {
		addExactTerm(field, String.valueOf(value));
	}

	@Override
	public void addExactTerm(String field, int value) {
		addExactTerm(field, String.valueOf(value));
	}

	@Override
	public void addExactTerm(String field, Integer value) {
		addExactTerm(field, String.valueOf(value));
	}

	@Override
	public void addExactTerm(String field, long value) {
		addExactTerm(field, String.valueOf(value));
	}

	@Override
	public void addExactTerm(String field, Long value) {
		addExactTerm(field, String.valueOf(value));
	}

	@Override
	public void addExactTerm(String field, short value) {
		addExactTerm(field, String.valueOf(value));
	}

	@Override
	public void addExactTerm(String field, Short value) {
		addExactTerm(field, String.valueOf(value));
	}

	@Override
	public void addExactTerm(String field, String value) {
		_boolQuery.should(QueryBuilders.termQuery(field, value));
	}

	@Override
	public void addNumericRangeTerm(
		String field, int startValue, int endValue) {

		addRangeQuery(field, startValue, endValue);
	}

	@Override
	public void addNumericRangeTerm(
		String field, Integer startValue, Integer endValue) {

		addRangeQuery(field, startValue, endValue);
	}

	@Override
	public void addNumericRangeTerm(
		String field, long startValue, long endValue) {

		addRangeQuery(field, startValue, endValue);
	}

	@Override
	public void addNumericRangeTerm(
		String field, Long startValue, Long endValue) {

		addRangeQuery(field, startValue, endValue);
	}

	@Override
	public void addNumericRangeTerm(
		String field, short startValue, short endValue) {

		addRangeQuery(field, startValue, endValue);
	}

	@Override
	public void addNumericRangeTerm(
		String field, Short startValue, Short endValue) {

		addRangeQuery(field, startValue, endValue);
	}

	@Override
	public void addRangeTerm(String field, int startValue, int endValue) {
		addRangeQuery(field, startValue, endValue);
	}

	@Override
	public void addRangeTerm(
		String field, Integer startValue, Integer endValue) {

		addRangeQuery(field, startValue, endValue);
	}

	@Override
	public void addRangeTerm(String field, long startValue, long endValue) {
		addRangeQuery(field, startValue, endValue);
	}

	@Override
	public void addRangeTerm(String field, Long startValue, Long endValue) {
		addRangeQuery(field, startValue, endValue);
	}

	@Override
	public void addRangeTerm(String field, short startValue, short endValue) {
		addRangeQuery(field, startValue, endValue);
	}

	@Override
	public void addRangeTerm(String field, Short startValue, Short endValue) {
		addRangeQuery(field, startValue, endValue);
	}

	@Override
	public void addRangeTerm(String field, String startValue, String endValue) {
		addRangeQuery(field, startValue, endValue);
	}

	@Override
	public void addRequiredTerm(String field, boolean value) {
		addRequiredTerm(field, String.valueOf(value), false);
	}

	@Override
	public void addRequiredTerm(String field, Boolean value) {
		addRequiredTerm(field, String.valueOf(value), false);
	}

	@Override
	public void addRequiredTerm(String field, double value) {
		addRequiredTerm(field, String.valueOf(value), false);
	}

	@Override
	public void addRequiredTerm(String field, Double value) {
		addRequiredTerm(field, String.valueOf(value), false);
	}

	@Override
	public void addRequiredTerm(String field, int value) {
		addRequiredTerm(field, String.valueOf(value), false);
	}

	@Override
	public void addRequiredTerm(String field, Integer value) {
		addRequiredTerm(field, String.valueOf(value), false);
	}

	@Override
	public void addRequiredTerm(String field, long value) {
		addRequiredTerm(field, String.valueOf(value), false);
	}

	@Override
	public void addRequiredTerm(String field, Long value) {
		addRequiredTerm(field, String.valueOf(value), false);
	}

	@Override
	public void addRequiredTerm(String field, short value) {
		addRequiredTerm(field, String.valueOf(value), false);
	}

	@Override
	public void addRequiredTerm(String field, Short value) {
		addRequiredTerm(field, String.valueOf(value), false);
	}

	@Override
	public void addRequiredTerm(String field, String value) {
		addRequiredTerm(field, String.valueOf(value), false);
	}

	@Override
	public void addRequiredTerm(String field, String value, boolean like) {
		addRequiredTerm(field, new String[] {value}, like);
	}

	public void addRequiredTerm(String field, String[] values, boolean like) {
		if (values == null) {
			return;
		}

		for (String value : values) {
			addTerm(field, value, like, BooleanClauseOccur.MUST);
		}
	}

	@Override
	public void addTerm(String field, long value) {
		addTerm(field, String.valueOf(value), false);
	}

	@Override
	public void addTerm(String field, String value) {
		addTerm(field, value, false);
	}

	@Override
	public void addTerm(String field, String value, boolean like) {
		addTerm(field, value, like, BooleanClauseOccur.SHOULD);
	}

	@Override
	public void addTerm(
		String field, String value, boolean like,
		BooleanClauseOccur booleanClauseOccur) {

		if (Validator.isNull(value)) {
			return;
		}

		Query query = FieldQueryFactoryUtil.createQuery(
			field, value, like, false);

		if (query != null) {
			add(query, booleanClauseOccur);
		}
		else {
			String[] values = StringUtil.split(
				value, ElasticsearchConstants.TERMS_DELIMITER);

			QueryBuilder queryBuilder = null;

			if (values.length > 1) {
				queryBuilder = QueryBuilders.termsQuery(field, values);
			}
			else {
				queryBuilder = QueryBuilders.termQuery(field, value);
			}

			if (booleanClauseOccur == BooleanClauseOccur.MUST) {
				_boolQuery.must(queryBuilder);
			}
			else {
				_boolQuery.should(queryBuilder);
			}
		}
	}

	@Override
	public List<BooleanClause> clauses() {
		return Collections.emptyList();
	}

	public BoolQueryBuilder getBoolQuery() {
		return _boolQuery;
	}

	@Override
	public Object getWrappedQuery() {
		return getBoolQuery();
	}

	@Override
	public boolean hasClauses() {
		return _boolQuery.hasClauses();
	}

	@Override
	public String toString() {
		return _boolQuery.toString();
	}

	protected void addRangeQuery(
		String field, Object startValue, Object endValue) {

		RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery(field);

		rangeQueryBuilder.from(startValue);
		rangeQueryBuilder.to(endValue);

		_boolQuery.should(rangeQueryBuilder);
	}

	private BoolQueryBuilder _boolQuery;

}