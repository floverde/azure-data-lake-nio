package com.github.floverde.azure.datalake.nio;

import com.azure.identity.ClientSecretCredential;
import com.azure.identity.ManagedIdentityCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.identity.ManagedIdentityCredentialBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.core.credential.AzureSasCredential;
import com.azure.core.credential.TokenCredential;
import com.azure.core.exception.AzureException;
import io.netty.util.internal.StringUtil;
import java.util.Collections;
import java.util.Objects;
import java.util.Map;

public final class ADLSConfigurationReader
{
    private final String authority;

    private final Map<String, ?> config;

    private static final String CLIENT_ID = "azure.client.id";

    private static final String TENANT_ID = "azure.tenant.id";

    private static final String SAS_TOKEN = "azure.sas.token";

    private static final String PRE_BUILT = "azure.credential";

    private static final String ACCOUNT_KEY = "azure.account.key";

    private static final String CLIENT_SECRET = "azure.client.secret";

    private static final String AUTO_MANAGED_IDENTITY = "azure.managed.identity.auto";

    private static final String MANAGED_IDENTITY_CLIENT_ID = "azure.managed.identity.client.id";

    public ADLSConfigurationReader(final Map<String, ?> env, final String authority) {
        this.config = env != null ? env : Collections.emptyMap();
        this.authority = authority;
    }

    public StorageSharedKeyCredential getSharedKeyCredential() {
        final String accountName, accountKey;
        // Extract account name from the authority provided
        accountName = StringUtil.substringBefore(this.authority, '.');
        // Get the account key from the configuration using the predefined key
        accountKey = this.getString(ADLSConfigurationReader.ACCOUNT_KEY);
        // Create a new StorageSharedKeyCredential using the extracted account name and key
        return new StorageSharedKeyCredential(accountName, accountKey);
    }

    public ClientSecretCredential getClientSecretCredential() {
        // Create a new ClientSecretCredential using the client ID, tenant ID, and client secret
        return new ClientSecretCredentialBuilder().clientId(this.getString(ADLSConfigurationReader.
                CLIENT_ID)).tenantId(this.getString(ADLSConfigurationReader.TENANT_ID)).clientSecret(
                this.getString(ADLSConfigurationReader.CLIENT_SECRET)).build();
    }

    public TokenCredential getPreBuiltCredential() {
        // Retrieve the pre-built credential from the configuration using the predefined key
        final Object credential = this.config.get(ADLSConfigurationReader.PRE_BUILT);
        // Ensure that the retrieved credential is not null and is an instance of TokenCredential
        if (!(credential instanceof TokenCredential)) {
            throw new AzureException(String.format("%s must be an instance of TokenCredential, got: %s",
                    ADLSConfigurationReader.PRE_BUILT, credential != null ? credential.getClass().
                    getName() : "null"));
        }
        // Return the pre-built credential cast to TokenCredential
        return (TokenCredential) credential;
    }

    public boolean hasManagedIdentityCredential() {
        return this.config.containsKey(ADLSConfigurationReader.MANAGED_IDENTITY_CLIENT_ID) || Boolean.
                parseBoolean(this.getString(ADLSConfigurationReader.AUTO_MANAGED_IDENTITY));
    }

    public ManagedIdentityCredential getManagedIdentityCredential() {
        return new ManagedIdentityCredentialBuilder().clientId(this.getString(
                ADLSConfigurationReader.MANAGED_IDENTITY_CLIENT_ID)).build();
    }

    public AzureSasCredential getSasCredential() {
        // Create a new AzureSasCredential using the SAS token from the configuration
        return new AzureSasCredential(this.getString(ADLSConfigurationReader.SAS_TOKEN));
    }

    public boolean hasClientSecretCredential() {
        return this.config.containsKey(ADLSConfigurationReader.CLIENT_ID) && this.
                config.containsKey(ADLSConfigurationReader.CLIENT_SECRET) && this.
                config.containsKey(ADLSConfigurationReader.TENANT_ID);
    }

    private String getString(final String key) {
        return Objects.toString(this.config.get(key), null);
    }

    public boolean hasSharedKeyCredential() {
        return this.config.containsKey(ADLSConfigurationReader.ACCOUNT_KEY);
    }

    public boolean hasPreBuiltCredential() {
        return this.config.containsKey(ADLSConfigurationReader.PRE_BUILT);
    }

    public boolean hasSasCredential() {
        return this.config.containsKey(ADLSConfigurationReader.SAS_TOKEN);
    }
}
