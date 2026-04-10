use actix_web::{HttpResponse, Responder};

pub async fn index_handler() -> impl Responder {
    let documentation = r#"
Omega API Server

Available Routes:
- /api/*    : API endpoints for the Omega server.
- /direct/* : Resolution for shortened links.

All other routes will return this documentation.
"#;

    HttpResponse::Ok()
        .content_type("text/plain; charset=utf-8")
        .body(documentation)
}
