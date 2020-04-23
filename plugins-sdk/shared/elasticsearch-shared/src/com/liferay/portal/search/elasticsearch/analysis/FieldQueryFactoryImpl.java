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

package com.liferay.portal.search.elasticsearch.analysis;

import com.liferay.portal.kernel.search.Query;

import java.util.HashSet;
import java.util.Map;

/**
 * @author Michael C. Han
 */
public class FieldQueryFactoryImpl implements FieldQueryFactory {

	@Override
	public Query createQuery(
		String fieldName, String keywords, boolean like,
		boolean splitKeywords) {

		FieldQueryBuilder fieldQueryBuilder = getQueryBuilder(fieldName);

		if (fieldQueryBuilder != null) {
			return fieldQueryBuilder.build(fieldName, keywords);
		}
		else {
			return null;
		}
	}

	public void setFieldQueryBuilderFactories(
		Map<String, FieldQueryBuilderFactory> fieldQueryBuilderFactories) {

		_fieldQueryBuilderFactories.addAll(fieldQueryBuilderFactories.values());
	}

	public void setTitleQueryBuilder(TitleFieldQueryBuilder titleQueryBuilder) {
		this.titleQueryBuilder = titleQueryBuilder;
	}

	protected FieldQueryBuilder getQueryBuilder(String fieldName) {
		for (FieldQueryBuilderFactory fieldQueryBuilderFactory :
				_fieldQueryBuilderFactories) {

			FieldQueryBuilder fieldQueryBuilder =
				fieldQueryBuilderFactory.getQueryBuilder(fieldName);

			if (fieldQueryBuilder != null) {
				return fieldQueryBuilder;
			}
		}

		return null;
	}

	protected TitleFieldQueryBuilder titleQueryBuilder;

	private final HashSet<FieldQueryBuilderFactory>
		_fieldQueryBuilderFactories = new HashSet<FieldQueryBuilderFactory>();

}