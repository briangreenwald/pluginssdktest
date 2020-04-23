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

package com.liferay.portal.search.elasticsearch.hook.action;

import com.liferay.portal.kernel.log.Log;
import com.liferay.portal.kernel.log.LogFactoryUtil;
import com.liferay.portal.kernel.servlet.SessionErrors;
import com.liferay.portal.kernel.struts.BaseStrutsPortletAction;
import com.liferay.portal.kernel.util.Constants;
import com.liferay.portal.kernel.util.ParamUtil;
import com.liferay.portal.kernel.util.PortalClassLoaderUtil;
import com.liferay.portal.kernel.util.ReflectionUtil;
import com.liferay.portal.kernel.util.Validator;
import com.liferay.portal.kernel.util.WebKeys;
import com.liferay.portal.model.Company;
import com.liferay.portal.search.elasticsearch.ElasticsearchSearchEngine;
import com.liferay.portal.search.elasticsearch.ElasticsearchSearchEngineUtil;
import com.liferay.portal.security.auth.PrincipalException;
import com.liferay.portal.security.permission.PermissionChecker;
import com.liferay.portal.service.CompanyLocalServiceUtil;
import com.liferay.portal.theme.ThemeDisplay;

import java.lang.reflect.Method;

import java.util.List;

import javax.portlet.ActionRequest;
import javax.portlet.ActionResponse;
import javax.portlet.PortletConfig;
import javax.portlet.PortletRequest;

/**
 * @author Wesley Gong
 */
public class ElasticsearchEditServerAction extends BaseStrutsPortletAction {

	@Override
	public void processAction(
			PortletConfig portletConfig, ActionRequest actionRequest,
			ActionResponse actionResponse)
		throws Exception {

		String cmd = ParamUtil.getString(actionRequest, Constants.CMD);
		String portletId = ParamUtil.getString(actionRequest, "portletId");

		if (cmd.equals("reindex") && Validator.isNull(portletId)) {
			ThemeDisplay themeDisplay =
				(ThemeDisplay)actionRequest.getAttribute(WebKeys.THEME_DISPLAY);

			PermissionChecker permissionChecker =
				themeDisplay.getPermissionChecker();

			if (!permissionChecker.isOmniadmin()) {
				SessionErrors.add(
					actionRequest, PrincipalException.class.getName());

				String forwardKey = (String)_getForwardKeyMethod.invoke(
					_portletActionClass.newInstance(), actionRequest);

				actionRequest.setAttribute(forwardKey, "portlet.admin.error");

				return;
			}

			reindex(actionRequest);

			_sendRedirectMethod.invoke(
				_portletActionClass.newInstance(), actionRequest,
				actionResponse);
		}
		else {
			_processActionMethod.invoke(
				_editServerActionClass.newInstance(), null, null, portletConfig,
				actionRequest, actionResponse);
		}
	}

	protected void reindex(ActionRequest actionRequest) throws Exception {
		try {
			List<Company> companies = CompanyLocalServiceUtil.getCompanies();

			ElasticsearchSearchEngine elasticsearchSearchEngine =
				ElasticsearchSearchEngineUtil.getElasticsearchEngine();

			for (Company company : companies) {
				elasticsearchSearchEngine.removeCompany(company.getCompanyId());

				elasticsearchSearchEngine.initialize(company.getCompanyId());
			}
		}
		catch (Exception e) {
			if (_log.isErrorEnabled()) {
				_log.error("Unable to remove and initialize indices", e);
			}
		}

		_reindexMethod.invoke(
			_editServerActionClass.newInstance(), actionRequest);
	}

	private static Log _log = LogFactoryUtil.getLog(
		ElasticsearchEditServerAction.class);

	private static Class<?> _editServerActionClass;
	private static Method _getForwardKeyMethod;
	private static Class<?> _portletActionClass;
	private static Method _processActionMethod;
	private static Method _reindexMethod;
	private static Method _sendRedirectMethod;

	static {
		try {
			ClassLoader portalClassLoader =
				PortalClassLoaderUtil.getClassLoader();

			_editServerActionClass = portalClassLoader.loadClass(
				"com.liferay.portlet.admin.action.EditServerAction");

			Class<?> actionFormClass = portalClassLoader.loadClass(
				"org.apache.struts.action.ActionForm");

			Class<?> actionMappingClass = portalClassLoader.loadClass(
				"org.apache.struts.action.ActionMapping");

			_processActionMethod = ReflectionUtil.getDeclaredMethod(
				_editServerActionClass, "processAction", actionMappingClass,
				actionFormClass, PortletConfig.class, ActionRequest.class,
				ActionResponse.class);

			_reindexMethod = ReflectionUtil.getDeclaredMethod(
				_editServerActionClass, "reindex", ActionRequest.class);

			_portletActionClass = portalClassLoader.loadClass(
				"com.liferay.portal.struts.PortletAction");

			_getForwardKeyMethod = ReflectionUtil.getDeclaredMethod(
				_portletActionClass, "getForwardKey", PortletRequest.class);

			_sendRedirectMethod = ReflectionUtil.getDeclaredMethod(
				_portletActionClass, "sendRedirect", ActionRequest.class,
				ActionResponse.class);
		}
		catch (Exception e) {
			_log.error(e, e);
		}
	}

}