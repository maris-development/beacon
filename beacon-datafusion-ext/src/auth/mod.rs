use std::sync::Arc;

use datafusion::common::HashMap;
use futures::future::BoxFuture;

pub struct AuthContext {
    role_provider: RoleProvider,
    auth_provider: Arc<dyn AuthProvider>,
}

impl AuthContext {
    pub fn new(role_provider: RoleProvider, auth_provider: Arc<dyn AuthProvider>) -> Self {
        Self {
            role_provider,
            auth_provider,
        }
    }

    pub fn role_provider(&self) -> &RoleProvider {
        &self.role_provider
    }

    pub fn auth_provider(&self) -> Arc<dyn AuthProvider> {
        self.auth_provider.clone()
    }

    pub fn has_privilege(&self, role_name: &str, privilege: &Privileges) -> bool {
        if let Some(role) = self.role_provider.roles.get(role_name) {
            role.privileges.contains(privilege)
        } else {
            false
        }
    }

    pub async fn authenticate(&self, auth_str: &str) -> anyhow::Result<Vec<Privileges>> {
        let roles = self.auth_provider.authenticate(auth_str).await?;
        let mut privileges = Vec::new();
        for role_name in roles {
            if let Some(role) = self.role_provider.roles.get(&role_name) {
                privileges.extend(role.privileges.clone());
            }
        }
        Ok(privileges)
    }
}

pub trait AuthProvider {
    fn authenticate(&self, auth_str: &str) -> BoxFuture<'_, anyhow::Result<Vec<String>>>;
}

pub struct RoleProvider {
    roles: HashMap<String, Role>,
}

pub struct Role {
    name: String,
    privileges: Vec<Privileges>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Privileges {
    Select,
    Insert,
    Update,
    Delete,
    Create,
    Drop,
    All,
}
