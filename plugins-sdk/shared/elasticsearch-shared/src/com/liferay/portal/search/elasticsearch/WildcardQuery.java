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

import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.WildcardQueryBuilder;

/**
 * @author Jenny Chen
 */
public class WildcardQuery extends BaseQueryImpl {

	public static final float BOOST_DEFAULT = 1.0f;

	public WildcardQuery(String field, String value) {
		_wildcardQueryBuilder = QueryBuilders.wildcardQuery(field, value);

		_field = field;
		_value = value;
	}

	public float getBoost() {
		return _boost;
	}

	public String getField() {
		return _field;
	}

	public String getValue() {
		return _value;
	}

	@Override
	public Object getWrappedQuery() {
		return _wildcardQueryBuilder;
	}

	public void setBoost(float boost) {
		_wildcardQueryBuilder.boost(boost);

		_boost = boost;
	}

	private float _boost = BOOST_DEFAULT;
	private final String _field;
	private final String _value;
	private WildcardQueryBuilder _wildcardQueryBuilder;

}