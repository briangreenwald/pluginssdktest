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

import com.liferay.compat.portal.kernel.util.ArrayUtil;
import com.liferay.compat.portal.kernel.util.StringUtil;
import com.liferay.portal.kernel.util.CharPool;
import com.liferay.portal.kernel.util.GetterUtil;
import com.liferay.util.portlet.PortletProps;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author André de Oliveira
 * @author Rodrigo Paulino
 */
public class FieldQueryBuilderFactoryImpl implements FieldQueryBuilderFactory {

	public void afterPropertiesSet() {
		Properties properties = PortletProps.getProperties();

		_descriptionFields = getFieldsAsList(
			properties, "elasticsearch.description.fields");
		_titleFields = getFieldsAsList(
			properties, "elasticsearch.title.fields");

		String[] extFieldNamePatterns = getFieldsAsArray(
			properties, "elasticsearch.field.name.patterns");

		String[] defaultFieldNamePatterns = new String[] {
			"assetCategoryTitles?(_.+)?", "assetTagNames", "emailAddress",
			"license", "path", "screenName", "tag", "treePath", "userName"
		};

		String[] fieldNamePatterns = ArrayUtil.append(
			extFieldNamePatterns, defaultFieldNamePatterns);

		for (String fieldNamePattern : fieldNamePatterns) {
			_fieldNamePatterns.put(
				fieldNamePattern, Pattern.compile(fieldNamePattern));
		}
	}

	@Override
	public FieldQueryBuilder getQueryBuilder(String field) {
		if (isSubstringSearchAlways(field)) {
			return substringQueryBuilder;
		}

		if (_descriptionFields.contains(field)) {
			return descriptionQueryBuilder;
		}

		if (_titleFields.contains(field)) {
			return titleQueryBuilder;
		}

		return null;
	}

	public boolean isSubstringSearchAlways(String fieldName) {
		if (_fieldNamePatterns.containsKey(fieldName)) {
			return true;
		}

		for (Pattern pattern : _fieldNamePatterns.values()) {
			Matcher matcher = pattern.matcher(fieldName);

			if (matcher.matches()) {
				return true;
			}
		}

		return false;
	}

	public void setDescriptionQueryBuilder(
		DescriptionFieldQueryBuilder descriptionQueryBuilder) {

		this.descriptionQueryBuilder = descriptionQueryBuilder;
	}

	public void setSubstringQueryBuilder(
		SubstringFieldQueryBuilder substringQueryBuilder) {

		this.substringQueryBuilder = substringQueryBuilder;
	}

	public void setTitleQueryBuilder(TitleFieldQueryBuilder titleQueryBuilder) {
		this.titleQueryBuilder = titleQueryBuilder;
	}

	protected String[] getFieldsAsArray(Properties properties, String key) {
		return StringUtil.split(
			GetterUtil.getString(properties.get(key)), CharPool.PIPE);
	}

	protected Collection<String> getFieldsAsList(
		Properties properties, String key) {

		String[] values = getFieldsAsArray(properties, key);

		return new HashSet<String>(Arrays.asList(values));
	}

	protected DescriptionFieldQueryBuilder descriptionQueryBuilder;
	protected SubstringFieldQueryBuilder substringQueryBuilder;
	protected TitleFieldQueryBuilder titleQueryBuilder;

	private volatile Collection<String> _descriptionFields =
		Collections.singleton("description");
	private final Map<String, Pattern> _fieldNamePatterns =
		new LinkedHashMap<String, Pattern>();
	private volatile Collection<String> _titleFields = Collections.singleton(
		"title");

}