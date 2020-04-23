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

package com.liferay.portal.search.elasticsearch.servlet;

import com.liferay.portal.kernel.log.Log;
import com.liferay.portal.kernel.log.LogFactoryUtil;
import com.liferay.portal.kernel.util.BasePortalLifecycle;
import com.liferay.portal.model.Company;
import com.liferay.portal.search.elasticsearch.ElasticsearchSearchEngine;
import com.liferay.portal.search.elasticsearch.ElasticsearchSearchEngineUtil;
import com.liferay.portal.service.CompanyLocalServiceUtil;

import java.util.List;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

/**
 * @author Michael C. Han
 */
public class ElasticsearchServletContextListener
	extends BasePortalLifecycle implements ServletContextListener {

	@Override
	public void contextDestroyed(ServletContextEvent servletContextEvent) {
		portalDestroy();
	}

	@Override
	public void contextInitialized(ServletContextEvent servletContextEvent) {
		registerPortalLifecycle();
	}

	@Override
	protected void doPortalDestroy() throws Exception {
	}

	@Override
	protected void doPortalInit() {
		try {
			List<Company> companies = CompanyLocalServiceUtil.getCompanies();

			ElasticsearchSearchEngine elasticsearchSearchEngine =
				ElasticsearchSearchEngineUtil.getElasticsearchEngine();

			for (Company company : companies) {
				elasticsearchSearchEngine.initialize(company.getCompanyId());
			}
		}
		catch (Exception e) {
			if (_log.isErrorEnabled()) {
				_log.error("Unable to initialize indices", e);
			}
		}
	}

	private static Log _log = LogFactoryUtil.getLog(
		ElasticsearchServletContextListener.class);

}