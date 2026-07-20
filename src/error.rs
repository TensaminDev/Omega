use thiserror::Error;

#[derive(Debug, Error)]
pub enum OmegaError {
    #[error("database pool is not initialized")]
    DatabaseNotInitialized,
    #[error("database error: {0}")]
    Database(sqlx::Error),
    #[error("invalid input: {0}")]
    Validation(String),
    #[error("resource not found")]
    NotFound,
    #[error("transport error: {0}")]
    Transport(String),
    #[error("not connected")]
    NotConnected,
    #[error("not authenticated")]
    NotAuthenticated,
    #[error("invalid response")]
    InvalidResponse,
    #[error("authentication failed")]
    AuthenticationFailed,
    #[error("send error: {0}")]
    SendError(String),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, OmegaError>;

impl From<sqlx::Error> for OmegaError {
    fn from(error: sqlx::Error) -> Self {
        if matches!(error, sqlx::Error::RowNotFound) {
            Self::NotFound
        } else {
            Self::Database(error)
        }
    }
}

impl OmegaError {
    pub fn status_code(&self) -> http::StatusCode {
        match self {
            Self::Validation(_) => http::StatusCode::BAD_REQUEST,
            Self::NotFound => http::StatusCode::NOT_FOUND,
            Self::DatabaseNotInitialized
            | Self::Database(_)
            | Self::Transport(_)
            | Self::NotConnected
            | Self::NotAuthenticated
            | Self::InvalidResponse
            | Self::AuthenticationFailed
            | Self::SendError(_)
            | Self::Io(_) => http::StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}
