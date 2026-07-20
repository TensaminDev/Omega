use http::StatusCode;
use mtp::webserver::Http3Response;

pub fn index_handler(response: Http3Response) -> Http3Response {
    let documentation = r#"
Omega API Server

Available Routes:
- /api/*    : API endpoints for the Omega server.
- /direct/* : Resolution for shortened links.

All other routes will return this documentation.
"#;

    response
        .status(StatusCode::OK)
        .header("content-type", "text/plain; charset=utf-8")
        .body(documentation)
}
