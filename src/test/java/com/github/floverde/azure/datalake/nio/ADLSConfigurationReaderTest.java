package com.github.floverde.azure.datalake.nio;

import com.azure.core.credential.AzureSasCredential;
import com.azure.core.credential.TokenCredential;
import com.azure.core.exception.AzureException;
import com.azure.identity.ClientSecretCredential;
import com.azure.identity.ManagedIdentityCredential;
import com.azure.storage.common.StorageSharedKeyCredential;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for {@link ADLSConfigurationReader}.
 */
public final class ADLSConfigurationReaderTest
{
    private static final String AUTHORITY = "myaccount.dfs.core.windows.net";

    // ---- Constructor ----

    @Test
    public void testConstructorWithNullEnvUsesEmptyMap() {
        final ADLSConfigurationReader reader = new ADLSConfigurationReader(null, AUTHORITY);
        assertFalse(reader.hasSharedKeyCredential());
        assertFalse(reader.hasSasCredential());
        assertFalse(reader.hasPreBuiltCredential());
        assertFalse(reader.hasClientSecretCredential());
        assertFalse(reader.hasManagedIdentityCredential());
    }

    @Test
    public void testConstructorWithEmptyEnv() {
        final ADLSConfigurationReader reader = new ADLSConfigurationReader(Collections.emptyMap(), AUTHORITY);
        assertFalse(reader.hasSharedKeyCredential());
        assertFalse(reader.hasSasCredential());
        assertFalse(reader.hasPreBuiltCredential());
        assertFalse(reader.hasClientSecretCredential());
        assertFalse(reader.hasManagedIdentityCredential());
    }

    // ---- hasSharedKeyCredential / getSharedKeyCredential ----

    @Test
    public void testHasSharedKeyCredentialReturnsTrueWhenKeyPresent() {
        final ADLSConfigurationReader reader = new ADLSConfigurationReader(
                Map.of("azure.account.key", "dGVzdGtleQ=="), AUTHORITY);
        assertTrue(reader.hasSharedKeyCredential());
    }

    @Test
    public void testHasSharedKeyCredentialReturnsFalseWhenKeyAbsent() {
        final ADLSConfigurationReader reader = new ADLSConfigurationReader(Map.of(), AUTHORITY);
        assertFalse(reader.hasSharedKeyCredential());
    }

    @Test
    public void testGetSharedKeyCredentialReturnsCorrectAccountName() {
        final ADLSConfigurationReader reader = new ADLSConfigurationReader(
                Map.of("azure.account.key", "dGVzdGtleQ=="), AUTHORITY);
        final StorageSharedKeyCredential credential = reader.getSharedKeyCredential();
        assertNotNull(credential);
        assertEquals("myaccount", credential.getAccountName());
    }

    @Test
    public void testGetSharedKeyCredentialExtractsAccountNameBeforeFirstDot() {
        final ADLSConfigurationReader reader = new ADLSConfigurationReader(
                Map.of("azure.account.key", "dGVzdGtleQ=="),
                "storage123.dfs.core.windows.net");
        final StorageSharedKeyCredential credential = reader.getSharedKeyCredential();
        assertEquals("storage123", credential.getAccountName());
    }

    // ---- hasSasCredential / getSasCredential ----

    @Test
    public void testHasSasCredentialReturnsTrueWhenKeyPresent() {
        final ADLSConfigurationReader reader = new ADLSConfigurationReader(
                Map.of("azure.sas.token", "?sv=2020-08-04&sig=test"), AUTHORITY);
        assertTrue(reader.hasSasCredential());
    }

    @Test
    public void testHasSasCredentialReturnsFalseWhenKeyAbsent() {
        final ADLSConfigurationReader reader = new ADLSConfigurationReader(Map.of(), AUTHORITY);
        assertFalse(reader.hasSasCredential());
    }

    @Test
    public void testGetSasCredentialReturnsAzureSasCredential() {
        final ADLSConfigurationReader reader = new ADLSConfigurationReader(
                Map.of("azure.sas.token", "?sv=2020-08-04&sig=test"), AUTHORITY);
        assertInstanceOf(AzureSasCredential.class, reader.getSasCredential());
    }

    // ---- hasPreBuiltCredential / getPreBuiltCredential ----

    @Test
    public void testHasPreBuiltCredentialReturnsTrueWhenKeyPresent() {
        final TokenCredential mockCredential = mock(TokenCredential.class);
        final ADLSConfigurationReader reader = new ADLSConfigurationReader(
                Map.of("azure.credential", mockCredential), AUTHORITY);
        assertTrue(reader.hasPreBuiltCredential());
    }

    @Test
    public void testHasPreBuiltCredentialReturnsFalseWhenKeyAbsent() {
        final ADLSConfigurationReader reader = new ADLSConfigurationReader(Map.of(), AUTHORITY);
        assertFalse(reader.hasPreBuiltCredential());
    }

    @Test
    public void testGetPreBuiltCredentialReturnsSameInstance() {
        final TokenCredential mockCredential = mock(TokenCredential.class);
        final ADLSConfigurationReader reader = new ADLSConfigurationReader(
                Map.of("azure.credential", mockCredential), AUTHORITY);
        assertSame(mockCredential, reader.getPreBuiltCredential());
    }

    @Test
    public void testGetPreBuiltCredentialThrowsWhenValueIsNotTokenCredential() {
        final ADLSConfigurationReader reader = new ADLSConfigurationReader(
                Map.of("azure.credential", "not-a-credential"), AUTHORITY);
        assertThrows(AzureException.class, reader::getPreBuiltCredential);
    }

    @Test
    public void testGetPreBuiltCredentialThrowsWhenValueIsNull() {
        final Map<String, Object> env = new java.util.HashMap<>();
        env.put("azure.credential", null);
        final ADLSConfigurationReader reader = new ADLSConfigurationReader(env, AUTHORITY);
        assertThrows(AzureException.class, reader::getPreBuiltCredential);
    }

    @Test
    public void testGetPreBuiltCredentialExceptionMessageContainsKeyName() {
        final ADLSConfigurationReader reader = new ADLSConfigurationReader(
                Map.of("azure.credential", 42), AUTHORITY);
        final AzureException ex = assertThrows(AzureException.class, reader::getPreBuiltCredential);
        assertTrue(ex.getMessage().contains("azure.credential"));
    }

    // ---- hasClientSecretCredential / getClientSecretCredential ----

    @Test
    public void testHasClientSecretCredentialReturnsTrueWhenAllThreeKeysPresent() {
        final ADLSConfigurationReader reader = new ADLSConfigurationReader(Map.of(
                "azure.client.id", "client-id",
                "azure.client.secret", "client-secret",
                "azure.tenant.id", "tenant-id"), AUTHORITY);
        assertTrue(reader.hasClientSecretCredential());
    }

    @Test
    public void testHasClientSecretCredentialReturnsFalseWhenClientIdMissing() {
        final ADLSConfigurationReader reader = new ADLSConfigurationReader(Map.of(
                "azure.client.secret", "client-secret",
                "azure.tenant.id", "tenant-id"), AUTHORITY);
        assertFalse(reader.hasClientSecretCredential());
    }

    @Test
    public void testHasClientSecretCredentialReturnsFalseWhenClientSecretMissing() {
        final ADLSConfigurationReader reader = new ADLSConfigurationReader(Map.of(
                "azure.client.id", "client-id",
                "azure.tenant.id", "tenant-id"), AUTHORITY);
        assertFalse(reader.hasClientSecretCredential());
    }

    @Test
    public void testHasClientSecretCredentialReturnsFalseWhenTenantIdMissing() {
        final ADLSConfigurationReader reader = new ADLSConfigurationReader(Map.of(
                "azure.client.id", "client-id",
                "azure.client.secret", "client-secret"), AUTHORITY);
        assertFalse(reader.hasClientSecretCredential());
    }

    @Test
    public void testGetClientSecretCredentialReturnsClientSecretCredential() {
        final ADLSConfigurationReader reader = new ADLSConfigurationReader(Map.of(
                "azure.client.id", "client-id",
                "azure.client.secret", "client-secret",
                "azure.tenant.id", "tenant-id"), AUTHORITY);
        assertInstanceOf(ClientSecretCredential.class, reader.getClientSecretCredential());
    }

    // ---- hasManagedIdentityCredential / getManagedIdentityCredential ----

    @Test
    public void testHasManagedIdentityCredentialReturnsTrueWhenClientIdPresent() {
        final ADLSConfigurationReader reader = new ADLSConfigurationReader(
                Map.of("azure.managed.identity.client.id", "mi-client-id"), AUTHORITY);
        assertTrue(reader.hasManagedIdentityCredential());
    }

    @Test
    public void testHasManagedIdentityCredentialReturnsTrueWhenAutoIsTrue() {
        final ADLSConfigurationReader reader = new ADLSConfigurationReader(
                Map.of("azure.managed.identity.auto", "true"), AUTHORITY);
        assertTrue(reader.hasManagedIdentityCredential());
    }

    @Test
    public void testHasManagedIdentityCredentialAutoIsCaseInsensitive() {
        assertTrue(new ADLSConfigurationReader(
                Map.of("azure.managed.identity.auto", "TRUE"), AUTHORITY)
                .hasManagedIdentityCredential());
        assertTrue(new ADLSConfigurationReader(
                Map.of("azure.managed.identity.auto", "True"), AUTHORITY)
                .hasManagedIdentityCredential());
    }

    @Test
    public void testHasManagedIdentityCredentialReturnsFalseWhenAutoIsFalse() {
        final ADLSConfigurationReader reader = new ADLSConfigurationReader(
                Map.of("azure.managed.identity.auto", "false"), AUTHORITY);
        assertFalse(reader.hasManagedIdentityCredential());
    }

    @Test
    public void testHasManagedIdentityCredentialReturnsFalseWhenNeitherKeyPresent() {
        final ADLSConfigurationReader reader = new ADLSConfigurationReader(Map.of(), AUTHORITY);
        assertFalse(reader.hasManagedIdentityCredential());
    }

    @Test
    public void testGetManagedIdentityCredentialReturnsManagedIdentityCredential() {
        final ADLSConfigurationReader reader = new ADLSConfigurationReader(
                Map.of("azure.managed.identity.client.id", "mi-client-id"), AUTHORITY);
        assertInstanceOf(ManagedIdentityCredential.class, reader.getManagedIdentityCredential());
    }

    @Test
    public void testGetManagedIdentityCredentialForSystemAssigned() {
        final ADLSConfigurationReader reader = new ADLSConfigurationReader(
                Map.of("azure.managed.identity.auto", "true"), AUTHORITY);
        assertInstanceOf(ManagedIdentityCredential.class, reader.getManagedIdentityCredential());
    }
}
