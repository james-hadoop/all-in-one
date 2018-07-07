package com.james.temp;

public class TenantNamespaceInfo {
    private String tenantId;
    private String namespaceId;
    private String tenantModelName;
    private String namespaceName;
    private String sessionDomain;

    public TenantNamespaceInfo(String tenantId, String namespaceId, String tenantModelName, String namespaceName,
            String sessionDomain) {
        this.tenantId = tenantId;
        this.namespaceId = namespaceId;
        this.tenantModelName = tenantModelName;
        this.namespaceName = namespaceName;
        this.sessionDomain = sessionDomain;
    }

    public TenantNamespaceInfo() {

    }

    public boolean isBlank() {
        if (null != tenantId && !tenantId.isEmpty() && null != namespaceId && !namespaceId.isEmpty()
                && null != tenantModelName && !tenantModelName.isEmpty() && null != namespaceName
                && !namespaceName.isEmpty() && null != sessionDomain && !sessionDomain.isEmpty()) {
            return true;
        }

        return false;
    }

    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    public String getNamespaceId() {
        return namespaceId;
    }

    public void setNamespaceId(String namespaceId) {
        this.namespaceId = namespaceId;
    }

    public String getTenantModelName() {
        return tenantModelName;
    }

    public void setTenantModelName(String tenantModelName) {
        this.tenantModelName = tenantModelName;
    }

    public String getNamespaceName() {
        return namespaceName;
    }

    public void setNamespaceName(String namespaceName) {
        this.namespaceName = namespaceName;
    }

    public String getSessionDomain() {
        return sessionDomain;
    }

    public void setSessionDomain(String sessionDomain) {
        this.sessionDomain = sessionDomain;
    }
}
