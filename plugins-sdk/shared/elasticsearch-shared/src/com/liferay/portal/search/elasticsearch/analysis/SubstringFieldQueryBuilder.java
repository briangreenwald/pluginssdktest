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

import com.liferay.compat.portal.kernel.util.StringUtil;
import com.liferay.portal.kernel.search.BooleanClauseOccur;
import com.liferay.portal.kernel.search.Query;
import com.liferay.portal.kernel.util.CharPool;
import com.liferay.portal.kernel.util.GetterUtil;
import com.liferay.portal.kernel.util.StringPool;
import com.liferay.portal.search.elasticsearch.BooleanQueryImpl;
import com.liferay.portal.search.elasticsearch.WildcardQuery;
import com.liferay.util.portlet.PortletProps;

import java.util.List;
import java.util.Properties;

/**
 * @author André de Oliveira
 * @author Rodrigo Paulino
 */
public class SubstringFieldQueryBuilder implements FieldQueryBuilder {

	@Override
	public Query build(String field, String keywords) {
		BooleanQueryImpl booleanQueryImpl = new BooleanQueryImpl();

		BooleanClauseOccur booleanClauseOccur = BooleanClauseOccur.SHOULD;

		Properties properties = PortletProps.getProperties();

		if (GetterUtil.getBoolean(
				properties.get("elasticsearch.boolean.clause.occur.must"))) {

			booleanClauseOccur = BooleanClauseOccur.MUST;
		}

		List<String> tokens = keywordTokenizer.tokenize(keywords);

		for (String token : tokens) {
			booleanQueryImpl.add(createQuery(field, token), booleanClauseOccur);
		}

		return booleanQueryImpl;
	}

	public void setKeywordTokenizer(KeywordTokenizer keywordTokenizer) {
		this.keywordTokenizer = keywordTokenizer;
	}

	protected Query createQuery(String field, String value) {
		if (StringUtil.startsWith(value, CharPool.QUOTE)) {
			value = StringUtil.unquote(value);
		}

		value = StringUtil.replace(value, CharPool.PERCENT, StringPool.BLANK);

		if (value.isEmpty()) {
			value = StringPool.STAR;
		}
		else {
			value = StringUtil.quote(
				StringUtil.lowerCase(value), StringPool.STAR);
		}

		return new WildcardQuery(field, value);
	}

	protected KeywordTokenizer keywordTokenizer;

}