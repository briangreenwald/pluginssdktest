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
import com.liferay.portal.kernel.util.GetterUtil;
import com.liferay.util.portlet.PortletProps;

/**
 * @author André de Oliveira
 * @author Rodrigo Paulino
 */
public class DescriptionFieldQueryBuilder implements FieldQueryBuilder {

	public void afterPropertiesSet() {
		_exactMatchBoost =
			GetterUtil.getFloat(
				PortletProps.get("elasticsearch.exact.match.boost"),
				_exactMatchBoost);

		_proximitySlop = GetterUtil.getInteger(
			PortletProps.get("elasticsearch.proximity.slop"), _proximitySlop);
	}

	@Override
	public Query build(String field, String keywords) {
		FullTextQueryBuilder fullTextQueryBuilder = new FullTextQueryBuilder(
			keywordTokenizer);

		fullTextQueryBuilder.setExactMatchBoost(_exactMatchBoost);
		fullTextQueryBuilder.setProximitySlop(_proximitySlop);

		return fullTextQueryBuilder.build(field, keywords);
	}

	public void setKeywordTokenizer(KeywordTokenizer keywordTokenizer) {
		this.keywordTokenizer = keywordTokenizer;
	}

	protected KeywordTokenizer keywordTokenizer;

	private volatile float _exactMatchBoost = 2.0f;
	private volatile int _proximitySlop = 50;

}