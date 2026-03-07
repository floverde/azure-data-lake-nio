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

/**
 * Reads authentication configuration from an environment map and provides methods to build
 * the corresponding Azure SDK credential objects.
 *
 * <p>An instance is constructed with the raw {@code env} map passed to
 * {@link AzureDataLakeFileSystemProvider#newFileSystem} together with the storage-account
 * authority (e.g. {@code myaccount.dfs.core.windows.net}).  The class exposes a pair of methods
 * for each supported credential type: a {@code has*} predicate that returns {@code true} when
 * all required keys are present, and a {@code get*} factory that constructs the corresponding
 * credential object.</p>
 *
 * <p>Supported environment map keys and their credential types:</p>
 * <ul>
 *   <li>{@value #ACCOUNT_KEY} — {@link StorageSharedKeyCredential} (account shared key).</li>
 *   <li>{@value #SAS_TOKEN} — {@link AzureSasCredential} (Shared Access Signature).</li>
 *   <li>{@value #PRE_BUILT} — a pre-built {@link TokenCredential} instance.</li>
 *   <li>{@value #CLIENT_ID} + {@value #CLIENT_SECRET} + {@value #TENANT_ID} —
 *       {@link ClientSecretCredential} (service principal).</li>
 *   <li>{@value #MANAGED_IDENTITY_CLIENT_ID} — {@link ManagedIdentityCredential}
 *       (user-assigned managed identity).</li>
 *   <li>{@value #AUTO_MANAGED_IDENTITY} set to {@code "true"} — {@link ManagedIdentityCredential}
 *       (system-assigned managed identity).</li>
 * </ul>
 */
public final class ADLSConfigurationReader
{
    /** Storage-account authority (e.g. {@code myaccount.dfs.core.windows.net}). */
    private final String authority;

    /** Immutable view of the environment map; never {@code null}. */
    private final Map<String, ?> config;

    /** Environment map key for the Azure AD application (client) identifier, used for service principal authentication. */
    private static final String CLIENT_ID = "azure.client.id";

    /** Environment map key for the Azure AD tenant identifier, used for service principal authentication. */
    private static final String TENANT_ID = "azure.tenant.id";

    /** Environment map key for the SAS token string. */
    private static final String SAS_TOKEN = "azure.sas.token";

    /** Environment map key for a pre-built {@link TokenCredential} instance. */
    private static final String PRE_BUILT = "azure.credential";

    /** Environment map key for the storage account shared key (Base64-encoded). */
    private static final String ACCOUNT_KEY = "azure.account.key";

    /** Environment map key for the service-principal client secret. */
    private static final String CLIENT_SECRET = "azure.client.secret";

    /**
     * Environment map key that enables system-assigned managed identity when set to {@code "true"}
     * (case-insensitive).
     */
    private static final String AUTO_MANAGED_IDENTITY = "azure.managed.identity.auto";

    /** Environment map key for the client ID of a user-assigned managed identity. */
    private static final String MANAGED_IDENTITY_CLIENT_ID = "azure.managed.identity.client.id";

    /**
     * Creates a new {@code ADLSConfigurationReader}.
     *
     * @param env       the environment map passed to
     *                  {@link AzureDataLakeFileSystemProvider#newFileSystem}; may be {@code null},
     *                  in which case it is treated as an empty map.
     * @param authority the storage-account authority extracted from the file-system URI
     *                  (e.g. {@code myaccount.dfs.core.windows.net}).
     */
    public ADLSConfigurationReader(final Map<String, ?> env, final String authority) {
        this.config = env != null ? env : Collections.emptyMap();
        this.authority = authority;
    }

    /**
     * Returns {@code true} if the environment map contains the shared-key credential key
     * ({@value #ACCOUNT_KEY}).
     *
     * @return {@code true} when a storage account shared key has been configured.
     */
    public boolean hasSharedKeyCredential() {
        return this.config.containsKey(ADLSConfigurationReader.ACCOUNT_KEY);
    }

    /**
     * Builds a {@link StorageSharedKeyCredential} from the {@value #ACCOUNT_KEY} entry in the
     * environment map.
     *
     * <p>The account name is derived by extracting the segment of {@code authority} that precedes
     * the first {@code '.'} character.</p>
     *
     * @return a new {@link StorageSharedKeyCredential} for the configured account.
     */
    public StorageSharedKeyCredential getSharedKeyCredential() {
        final String accountName, accountKey;
        // Extract account name from the authority provided
        accountName = StringUtil.substringBefore(this.authority, '.');
        // Get the account key from the configuration using the predefined key
        accountKey = this.getString(ADLSConfigurationReader.ACCOUNT_KEY);
        // Create a new StorageSharedKeyCredential using the extracted account name and key
        return new StorageSharedKeyCredential(accountName, accountKey);
    }

    /**
     * Returns {@code true} if the environment map contains the SAS token key
     * ({@value #SAS_TOKEN}).
     *
     * @return {@code true} when a SAS token has been configured.
     */
    public boolean hasSasCredential() {
        return this.config.containsKey(ADLSConfigurationReader.SAS_TOKEN);
    }

    /**
     * Builds an {@link AzureSasCredential} from the {@value #SAS_TOKEN} entry in the environment
     * map.
     *
     * @return a new {@link AzureSasCredential} for the configured SAS token.
     */
    public AzureSasCredential getSasCredential() {
        // Create a new AzureSasCredential using the SAS token from the configuration
        return new AzureSasCredential(this.getString(ADLSConfigurationReader.SAS_TOKEN));
    }

    /**
     * Returns {@code true} if the environment map contains the pre-built credential key
     * ({@value #PRE_BUILT}).
     *
     * @return {@code true} when a pre-built {@link TokenCredential} has been configured.
     */
    public boolean hasPreBuiltCredential() {
        return this.config.containsKey(ADLSConfigurationReader.PRE_BUILT);
    }

    /**
     * Retrieves the pre-built {@link TokenCredential} from the {@value #PRE_BUILT} entry in the
     * environment map.
     *
     * @return the configured {@link TokenCredential} instance.
     * @throws AzureException if the value stored under {@value #PRE_BUILT} is {@code null} or is
     *                        not an instance of {@link TokenCredential}.
     */
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

    /**
     * Returns {@code true} if the environment map contains all three service-principal keys:
     * {@value #CLIENT_ID}, {@value #CLIENT_SECRET}, and {@value #TENANT_ID}.
     *
     * @return {@code true} when a complete service-principal (client-secret) configuration is
     *         present.
     */
    public boolean hasClientSecretCredential() {
        return this.config.containsKey(ADLSConfigurationReader.CLIENT_ID) && this.
                config.containsKey(ADLSConfigurationReader.CLIENT_SECRET) && this.
                config.containsKey(ADLSConfigurationReader.TENANT_ID);
    }

    /**
     * Builds a {@link ClientSecretCredential} from the {@value #CLIENT_ID},
     * {@value #CLIENT_SECRET}, and {@value #TENANT_ID} entries in the environment map.
     *
     * @return a new {@link ClientSecretCredential} for the configured service principal.
     */
    public ClientSecretCredential getClientSecretCredential() {
        // Create a new ClientSecretCredential using the client ID, tenant ID, and client secret
        return new ClientSecretCredentialBuilder().clientId(this.getString(ADLSConfigurationReader.
                CLIENT_ID)).tenantId(this.getString(ADLSConfigurationReader.TENANT_ID)).clientSecret(
                this.getString(ADLSConfigurationReader.CLIENT_SECRET)).build();
    }

    /**
     * Returns {@code true} if a managed identity credential has been configured — either a
     * user-assigned identity (via {@value #MANAGED_IDENTITY_CLIENT_ID}) or system-assigned
     * (via {@value #AUTO_MANAGED_IDENTITY} set to {@code "true"}, case-insensitive).
     *
     * @return {@code true} when managed-identity authentication has been configured.
     */
    public boolean hasManagedIdentityCredential() {
        return this.config.containsKey(ADLSConfigurationReader.MANAGED_IDENTITY_CLIENT_ID) || Boolean.
                parseBoolean(this.getString(ADLSConfigurationReader.AUTO_MANAGED_IDENTITY));
    }

    /**
     * Builds a {@link ManagedIdentityCredential}.
     *
     * <p>When {@value #MANAGED_IDENTITY_CLIENT_ID} is present in the environment map the
     * credential is configured for that specific user-assigned identity; otherwise a
     * system-assigned identity credential is returned.</p>
     *
     * @return a new {@link ManagedIdentityCredential} for the configured managed identity.
     */
    public ManagedIdentityCredential getManagedIdentityCredential() {
        return new ManagedIdentityCredentialBuilder().clientId(this.getString(
                ADLSConfigurationReader.MANAGED_IDENTITY_CLIENT_ID)).build();
    }

    /**
     * Returns the string value associated with {@code key} in the configuration map, or
     * {@code null} if the key is absent or mapped to {@code null}.
     *
     * @param key the configuration key to look up.
     * @return the string representation of the value, or {@code null}.
     */
    private String getString(final String key) {
        return Objects.toString(this.config.get(key), null);
    }
}
