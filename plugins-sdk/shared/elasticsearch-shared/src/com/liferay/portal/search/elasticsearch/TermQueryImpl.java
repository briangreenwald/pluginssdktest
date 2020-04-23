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

import com.liferay.portal.kernel.search.BaseQueryImpl;
import com.liferay.portal.kernel.search.QueryTerm;
import com.liferay.portal.kernel.search.TermQuery;

import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;

/**
 * @author Allen Ziegenfus
 */
public class TermQueryImpl extends BaseQueryImpl implements TermQuery {

	public static final float BOOST_DEFAULT = 1.0f;

	public TermQueryImpl(String field, long value) {
		this(field, String.valueOf(value));
	}

	public TermQueryImpl(String field, String value) {
		_termQueryBuilder = QueryBuilders.termQuery(field, value);

		_field = field;
		_value = value;
	}

	public float getBoost() {
		return _boost;
	}

	public String getField() {
		return _field;
	}

	@Override
	public QueryTerm getQueryTerm() {
		throw new UnsupportedOperationException();
	}

	public String getValue() {
		return _value;
	}

	@Override
	public Object getWrappedQuery() {
		return _termQueryBuilder;
	}

	public void setBoost(float boost) {
		_termQueryBuilder.boost(boost);

		_boost = boost;
	}

	private float _boost = BOOST_DEFAULT;
	private final String _field;
	private TermQueryBuilder _termQueryBuilder;
	private final String _value;

}