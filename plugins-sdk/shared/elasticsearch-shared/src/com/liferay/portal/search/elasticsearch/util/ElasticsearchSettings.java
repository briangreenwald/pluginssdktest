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

package com.liferay.portal.search.elasticsearch.util;

import java.util.concurrent.TimeUnit;

import org.elasticsearch.common.unit.TimeValue;

/**
 * @author Matthew Kong
 */
public class ElasticsearchSettings {

	public static final TimeValue SEARCH_SCROLL_KEEP_ALIVE_TIME_VALUE =
		new TimeValue(1, TimeUnit.MINUTES);

	public static final int SEARCH_SCROLL_SIZE = 1000;

	public static String getIndexPrefix() {
		return _indexPrefix;
	}

	public void setIndexPrefix(String indexPrefix) {
		_indexPrefix = indexPrefix;
	}

	private static String _indexPrefix;

}