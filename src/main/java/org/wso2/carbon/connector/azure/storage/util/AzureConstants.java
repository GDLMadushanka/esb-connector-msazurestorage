/*
 * Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.wso2.carbon.connector.azure.storage.util;

/**
 * This class contains the azure connector specific constants.
 */
public class AzureConstants {
    public static final String CONNECTOR_NAME = "azurestorage";
    public static final String CONNECTION_NAME = "name";
    public static final String ACCOUNT_NAME = "accountName";
    public static final String CLIENT_ID = "clientId";
    public static final String CLIENT_SECRET = "clientSecret";
    public static final String TENANT_ID = "tenantId";
    public static final String ACCOUNT_KEY = "accountKey";
    public static final String PROTOCOL = "defaultEndpointsProtocol";
    public static final String DEFAULT_PROTOCOL = "http";
    public static final String CONTAINER_NAME = "containerName";
    public static final String DESTINATION_FILE_PATH = "destinationFilePath";
    public static final String FILE_NAME = "fileName";
    public static final String FILE_PATH = "filePath";
    public static final String BLOB_CONTENT_TYPE = "blobContentType";
    public static final String TEXT_CONTENT = "textContent";
    public static final String ENDPOINT_PARAM = "DefaultEndpointsProtocol=http;AccountName=";
    public static final String ACCOUNT_NAME_PARAM = "AccountName=";
    public static final String ACCOUNT_KEY_PARAM = "AccountKey=";
    public static final String PROTOCOL_KEY_PARAM = "DefaultEndpointsProtocol=";
    public static final String SEMICOLON = ";";
    public static final String NAMESPACE = "ns";
    public static final String AZURE_NAMESPACE = "http://org.wso2.esbconnectors.msazurestorageconnector";
    public static final String BLOB = "blob";
    public static final String CONTAINER = "container";
    public static final String RESULT = "result";
    public static final String START_TAG = "<jsonObject><result><success>";
    public static final String END_TAG = "</success></result></jsonObject>";
    public static final String EMPTY_RESULT_TAG = "<result></></result>";

    public static final String METADATA = "metadata";
    public static final String START_TAG_ERROR = "<jsonObject><result><success>false</success><description>";
    public static final String END_TAG_ERROR = "</description></result></jsonObject>";
    public static final String ERR_CONTAINER_ALREADY_EXISTS = "CONTAINER_ALREADY_EXISTS";
    public static final String ERR_CONTAINER_DOES_NOT_EXIST = "CONTAINER_DOES_NOT_EXIST";
    public static final String ERR_UNKNOWN_ERROR_OCCURRED = "UNKNOWN_ERROR_OCCURRED";
    public static final String STATUS_SUCCESS = "SUCCESS";
    public static final String ERR_BLOB_DOES_NOT_EXIST = "BLOB_DOES_NOT_EXIST";
    public static final String BLOB_ENDPOINT_SUFFIX = ".blob.core.windows.net";
    public static final String HTTPS_PROTOCOL = "https://";
    public static final String PROPERTY_ERROR_CODE = "ERROR_CODE";
    public static final String PROPERTY_ERROR_MESSAGE = "ERROR_MESSAGE";
    public static final String PROPERTY_ERROR_DETAIL = "ERROR_DETAIL";
    public static final String BLOB_DOWNLOAD_FAILED = "BLOB_DOWNLOAD_FAILED";
    public static final String BLOB_DOWNLOAD_SUCCESSFUL = "BLOB_DOWNLOAD_SUCCESSFUL";
    public static final String METADATA_UPLOAD_FAILED = "METADATA_UPLOAD_FAILED";
    public static final String BLOB_DELETE_FAILED = "BLOB_DELETE_FAILED";
    public static final String ERROR_LOG_PREFIX = "Azure Storage connector encountered an error: ";
    private AzureConstants() {
    }
}
