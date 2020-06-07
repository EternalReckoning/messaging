use lapin::Error as LapinError;

pub enum Error {
    UnableToConnect(LapinError),
    ChannelCreationFailed(LapinError),
    QueueDeclareFailed(LapinError),
    PublishFailed(LapinError),
    AckNotReceived(LapinError),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::UnableToConnect(err) =>
                write!(f, "Unable to connect to broker: {}", err),
            Error::ChannelCreationFailed(err) =>
                write!(f, "Channel creation failed: {}", err),
            Error::QueueDeclareFailed(err) =>
                write!(f, "Failed to declare queue: {}", err),
            Error::PublishFailed(err) =>
                write!(f, "Failed to publish a message: {}", err),
            Error::AckNotReceived(err) =>
                write!(f, "No confirmation received for sent message: {}", err),
        }
    }
}
