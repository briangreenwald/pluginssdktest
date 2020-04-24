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

package com.liferay.portal.search.elasticsearch.index;

import com.liferay.portal.kernel.log.Log;
import com.liferay.portal.kernel.log.LogFactoryUtil;
import com.liferay.portal.kernel.util.StringUtil;
import com.liferay.portal.kernel.util.Validator;
import com.liferay.portal.search.elasticsearch.util.ElasticsearchSettings;
import com.liferay.portal.search.elasticsearch.util.LogUtil;

import java.io.InputStream;

import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.common.settings.Settings;

/**
 * @author Michael C. Han
 */
public class CompanyIndexFactory implements IndexFactory {

	@Override
	public void createIndices(AdminClient adminClient, long companyId)
		throws Exception {

		IndicesAdminClient indicesAdminClient = adminClient.indices();

		if (hasIndex(indicesAdminClient, companyId)) {
			return;
		}

		CreateIndexRequestBuilder createIndexRequestBuilder =
			indicesAdminClient.prepareCreate(
				ElasticsearchSettings.getIndexPrefix() + companyId);

		if (Validator.isNotNull(_indexConfigFileName)) {
			Settings.Builder builder = Settings.settingsBuilder();

			Class<?> clazz = getClass();

			ClassLoader classLoader = clazz.getClassLoader();

			InputStream inputStream = classLoader.getResourceAsStream(
				_indexConfigFileName);

			builder.loadFromStream(_indexConfigFileName, inputStream);

			createIndexRequestBuilder.setSettings(builder);
		}

		for (Map.Entry<String, String> entry : _typeMappings.entrySet()) {
			Class<?> clazz = getClass();

			String typeMapping = StringUtil.read(
				clazz.getClassLoader(), entry.getValue());

			createIndexRequestBuilder.addMapping(entry.getKey(), typeMapping);
		}

		CreateIndexResponse createIndexResponse =
			createIndexRequestBuilder.get();

		LogUtil.logActionResponse(_log, createIndexResponse);
	}

	@Override
	public void deleteIndices(AdminClient adminClient, long companyId)
		throws Exception {

		IndicesAdminClient indicesAdminClient = adminClient.indices();

		if (!hasIndex(indicesAdminClient, companyId)) {
			return;
		}

		DeleteIndexRequestBuilder deleteIndexRequestBuilder =
			indicesAdminClient.prepareDelete(
				ElasticsearchSettings.getIndexPrefix() + companyId);

		DeleteIndexResponse deleteIndexResponse =
			deleteIndexRequestBuilder.get();

		LogUtil.logActionResponse(_log, deleteIndexResponse);
	}

	public void setIndexConfigFileName(String indexConfigFileName) {
		_indexConfigFileName = indexConfigFileName;
	}

	public void setTypeMappings(Map<String, String> typeMappings) {
		_typeMappings = typeMappings;
	}

	protected boolean hasIndex(
			IndicesAdminClient indicesAdminClient, long companyId)
		throws Exception {

		IndicesExistsRequestBuilder indicesExistsRequestBuilder =
			indicesAdminClient.prepareExists(
				ElasticsearchSettings.getIndexPrefix() + companyId);

		IndicesExistsResponse indicesExistsResponse =
			indicesExistsRequestBuilder.get();

		return indicesExistsResponse.isExists();
	}

	private static Log _log = LogFactoryUtil.getLog(CompanyIndexFactory.class);

	private String _indexConfigFileName;
	private Map<String, String> _typeMappings = new HashMap<String, String>();

}