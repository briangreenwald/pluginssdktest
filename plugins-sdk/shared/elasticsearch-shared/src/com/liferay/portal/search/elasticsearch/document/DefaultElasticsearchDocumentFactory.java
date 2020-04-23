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

package com.liferay.portal.search.elasticsearch.document;

import com.liferay.portal.kernel.search.Document;
import com.liferay.portal.kernel.search.DocumentImpl;
import com.liferay.portal.kernel.search.Field;
import com.liferay.portal.kernel.util.LocaleUtil;
import com.liferay.portal.kernel.util.StringPool;
import com.liferay.portal.kernel.util.StringUtil;
import com.liferay.portal.kernel.util.Validator;

import java.io.IOException;

import java.math.BigDecimal;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

/**
 * @author Michael C. Han
 * @author Milen Dyankov
 */
public class DefaultElasticsearchDocumentFactory
	implements ElasticsearchDocumentFactory {

	@Override
	public String getElasticsearchDocument(Document document)
		throws IOException {

		XContentBuilder xContentBuilder = XContentFactory.jsonBuilder();

		xContentBuilder.startObject();

		Map<String, Field> fields = document.getFields();

		addFields(fields.values(), xContentBuilder);

		xContentBuilder.endObject();

		return xContentBuilder.string();
	}

	protected void addField(XContentBuilder xContentBuilder, Field field)
		throws IOException {

		String name = field.getName();

		if (name.contains(StringPool.SLASH)) {
			name = StringUtil.replace(
				name, StringPool.SLASH, StringPool.DOUBLE_UNDERLINE);
		}

		if (!field.isLocalized()) {
			String[] values = field.getValues();

			List<String> valuesList = new ArrayList<String>(values.length);

			for (String value : values) {
				if (Validator.isNull(value)) {
					continue;
				}

				valuesList.add(value.trim());
			}

			if (valuesList.isEmpty()) {
				return;
			}

			values = valuesList.toArray(new String[valuesList.size()]);

			addField(xContentBuilder, field, name, values);

			if (field.isSortable()) {
				String sortFieldName = DocumentImpl.getSortableFieldName(name);

				addField(xContentBuilder, field, sortFieldName, values);
			}
		}
		else {
			Map<Locale, String> localizedValues = field.getLocalizedValues();

			for (Map.Entry<Locale, String> entry : localizedValues.entrySet()) {
				String value = entry.getValue();

				if (Validator.isNull(value)) {
					continue;
				}

				Locale locale = entry.getKey();

				String languageId = LocaleUtil.toLanguageId(locale);

				String defaultLanguageId = LocaleUtil.toLanguageId(
					LocaleUtil.getDefault());

				value = value.trim();

				if (languageId.equals(defaultLanguageId)) {
					addField(xContentBuilder, field, name, value);
				}

				String localizedName = DocumentImpl.getLocalizedName(
					languageId, name);

				addField(xContentBuilder, field, localizedName, value);

				if (field.isSortable()) {
					String sortableFieldName =
						DocumentImpl.getSortableFieldName(localizedName);

					addField(xContentBuilder, field, sortableFieldName, value);
				}
			}
		}
	}

	protected void addField(
			XContentBuilder xContentBuilder, Field field, String fieldName,
			String... values)
		throws IOException {

		xContentBuilder.field(fieldName);

		xContentBuilder.startArray();

		for (String value : values) {
			xContentBuilder.value(translateValue(field, value));
		}

		xContentBuilder.endArray();
	}

	protected void addFields(
			Collection<Field> fields, XContentBuilder xContentBuilder)
		throws IOException {

		for (Field field : fields) {
			addField(xContentBuilder, field);
		}
	}

	protected Object translateValue(Field field, String value) {
		if (!field.isNumeric()) {
			return value;
		}
		else {
			Class<?> numericClass = field.getNumericClass();

			if (numericClass.equals(BigDecimal.class)) {
				return new BigDecimal(value);
			}
			else if (numericClass.equals(Double.class)) {
				return Double.valueOf(value);
			}
			else if (numericClass.equals(Float.class)) {
				return Float.valueOf(value);
			}
			else if (numericClass.equals(Integer.class)) {
				return Integer.valueOf(value);
			}
			else if (numericClass.equals(Short.class)) {
				return Short.valueOf(value);
			}
			else {
				return Long.valueOf(value);
			}
		}
	}

}