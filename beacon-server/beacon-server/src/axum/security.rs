//! OpenAPI security schemes shared by the client and admin documents.

use utoipa::{
    openapi::security::{Http, HttpAuthScheme, HttpBuilder, SecurityScheme},
    Modify,
};

/// Name of the HTTP Basic scheme, as endpoint `security` blocks reference it.
pub(super) const BASIC_AUTH: &str = "basic-auth";

/// Name of the bearer token scheme, as endpoint `security` blocks reference it.
pub(super) const BEARER: &str = "bearer";

/// Registers the `basic-auth` and `bearer` schemes in an OpenAPI document.
///
/// Every route parses both header forms through `credential_from_header`, so
/// both schemes apply to the client and admin surfaces alike. Swagger UI only
/// shows a scheme in its Authorize dialog once it is registered here.
pub(super) struct SecurityAddon;

impl Modify for SecurityAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        let components = openapi.components.get_or_insert_with(Default::default);
        components.add_security_scheme(
            BASIC_AUTH,
            SecurityScheme::Http(Http::new(HttpAuthScheme::Basic)),
        );
        components.add_security_scheme(
            BEARER,
            SecurityScheme::Http(
                HttpBuilder::new()
                    .scheme(HttpAuthScheme::Bearer)
                    .bearer_format("JWT")
                    .description(Some(
                        "OIDC access token, sent as `Authorization: Bearer <token>`.",
                    ))
                    .build(),
            ),
        );
    }
}
