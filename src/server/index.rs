use http::StatusCode;
use mtp::webserver::HttpResponse;

pub fn index_handler(response: HttpResponse) -> HttpResponse {
    let documentation = r#"
Omega API Server

Available Routes:
- /api/get/omikron
    Returns a randomly selected connected Omikron.
- /api/get/omikron/{id}
    Returns an Omikron by ID. If {id} is a connected Iota ID or user ID,
    returns that account's primary connected Omikron instead.
- /api/get/connections
    Returns the current live connection map as
    { "status": "success", "connections": { omikron_id: { iota_id: [user_id] } } }.
- /api/get/iota/{id}
    Returns the Iota's public identity data.
- /api/get/user/{id}
    Returns user profile and public identity data.
- /api/get/id/{username}
    Resolves a username to its user and Iota IDs.
- /api/get/public_key
    Returns Omega's public key.
- /api/download/iota_frontend
    Downloads the Iota frontend archive.
- /direct/{short_key}
    Resolves a shortened link with a temporary redirect.

All IDs must be positive decimal integers. The connections endpoint reports
live in-memory state; it is empty after an Omega restart until Omikrons sync.

All other routes will return this documentation.
"#;

    response
        .status(StatusCode::OK)
        .header("content-type", "text/plain; charset=utf-8")
        .body(documentation)
}
