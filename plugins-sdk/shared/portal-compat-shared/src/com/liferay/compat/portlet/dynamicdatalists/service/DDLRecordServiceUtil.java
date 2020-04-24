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

package com.liferay.compat.portlet.dynamicdatalists.service;

import com.liferay.portal.kernel.util.PortalClassLoaderUtil;
import com.liferay.portal.security.permission.ActionKeys;
import com.liferay.portal.security.permission.PermissionChecker;
import com.liferay.portal.security.permission.PermissionThreadLocal;
import com.liferay.portlet.dynamicdatalists.model.DDLRecord;
import com.liferay.portlet.dynamicdatalists.service.DDLRecordLocalServiceUtil;
import com.liferay.portlet.dynamicdatalists.service.DDLRecordSetServiceUtil;

import java.lang.reflect.Method;

/**
 * @author Marcellus Tavares
 */
public class DDLRecordServiceUtil extends DDLRecordSetServiceUtil {

	public static void deleteRecord(long recordId) throws Exception {
		DDLRecord record = DDLRecordLocalServiceUtil.getDDLRecord(recordId);

		ClassLoader classLoader = PortalClassLoaderUtil.getClassLoader();

		Class<?> ddlRecordSetPermissionClass = classLoader.loadClass(
			"com.liferay.portlet.dynamicdatalists.service.permission." +
				"DDLRecordSetPermission");

		Method checkMethod = ddlRecordSetPermissionClass.getMethod(
			"check", PermissionChecker.class, long.class, String.class);

		checkMethod.invoke(
			null, PermissionThreadLocal.getPermissionChecker(),
			record.getRecordSetId(), ActionKeys.DELETE);

		DDLRecordLocalServiceUtil.deleteDDLRecord(recordId);
	}

}